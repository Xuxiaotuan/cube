use cubestore::cluster::message::NetworkMessage;
use cubestore::metastore::{MetaStoreRpcMethodCall, MetaStoreRpcMethodResult};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const MAGIC: u32 = 94107;
const NETWORK_MESSAGE_VERSION: u32 = 1;

const OLD_NOTIFY_JOB_LISTENERS: &str =
    "124e6f746966794a6f624c697374656e65727300131401";
const OLD_NOTIFY_JOB_LISTENERS_SUCCESS: &str =
    "194e6f746966794a6f624c697374656e65727353756363657373001a1401";
const OLD_METASTORE_CALL: &str =
    "4d65746153746f726543616c6c001777616974466f7243757272656e74536571546f53796e630001280101011d14022401";
const OLD_METASTORE_CALL_RESULT: &str =
    "4d65746153746f726543616c6c526573756c740077616974466f7243757272656e74536571546f53796e63004f6b000104010101000001230101010724013e0101010724022401";

fn encode(message: &NetworkMessage) -> Vec<u8> {
    let mut serializer = flexbuffers::FlexbufferSerializer::new();
    message.serialize(&mut serializer).unwrap();
    serializer.take_buffer()
}

fn fixture(hex: &str) -> Vec<u8> {
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
        .collect()
}

fn decode(hex: &str) -> NetworkMessage {
    NetworkMessage::deserialize(flexbuffers::Reader::get_root(&fixture(hex)).unwrap()).unwrap()
}

async fn connected_streams() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let sender = TcpStream::connect(address).await.unwrap();
    let (receiver, _) = listener.accept().await.unwrap();
    (sender, receiver)
}

async fn write_frame(sender: &mut TcpStream, version: u32, payload: &[u8]) {
    sender.write_u32(MAGIC).await.unwrap();
    sender.write_u32(version).await.unwrap();
    sender.write_u64(payload.len() as u64).await.unwrap();
    sender.write_all(payload).await.unwrap();
}

#[derive(Serialize)]
enum FutureNetworkMessage {
    RouterInfoV2,
}

fn unknown_message_fixture() -> Vec<u8> {
    let mut serializer = flexbuffers::FlexbufferSerializer::new();
    FutureNetworkMessage::RouterInfoV2
        .serialize(&mut serializer)
        .unwrap();
    serializer.take_buffer()
}

#[test]
fn notify_job_listeners_preserves_master_encoding() {
    assert_eq!(
        encode(&NetworkMessage::NotifyJobListeners),
        fixture(OLD_NOTIFY_JOB_LISTENERS)
    );
    assert!(matches!(
        decode(OLD_NOTIFY_JOB_LISTENERS),
        NetworkMessage::NotifyJobListeners
    ));
}

#[test]
fn notify_job_listeners_success_preserves_master_encoding() {
    assert_eq!(
        encode(&NetworkMessage::NotifyJobListenersSuccess),
        fixture(OLD_NOTIFY_JOB_LISTENERS_SUCCESS)
    );
    assert!(matches!(
        decode(OLD_NOTIFY_JOB_LISTENERS_SUCCESS),
        NetworkMessage::NotifyJobListenersSuccess
    ));
}

#[test]
fn metastore_call_preserves_master_encoding() {
    let message = NetworkMessage::MetaStoreCall(MetaStoreRpcMethodCall::waitForCurrentSeqToSync);
    assert_eq!(encode(&message), fixture(OLD_METASTORE_CALL));
    assert!(matches!(
        decode(OLD_METASTORE_CALL),
        NetworkMessage::MetaStoreCall(MetaStoreRpcMethodCall::waitForCurrentSeqToSync)
    ));
}

#[test]
fn metastore_call_result_preserves_master_encoding() {
    let message = NetworkMessage::MetaStoreCallResult(
        MetaStoreRpcMethodResult::waitForCurrentSeqToSync(Ok(())),
    );
    assert_eq!(encode(&message), fixture(OLD_METASTORE_CALL_RESULT));
    assert!(matches!(
        decode(OLD_METASTORE_CALL_RESULT),
        NetworkMessage::MetaStoreCallResult(
            MetaStoreRpcMethodResult::waitForCurrentSeqToSync(Ok(()))
        )
    ));
}

#[tokio::test]
async fn send_writes_complete_v1_frame() {
    let (mut sender, mut receiver) = connected_streams().await;
    NetworkMessage::NotifyJobListeners.send(&mut sender).await.unwrap();

    assert_eq!(receiver.read_u32().await.unwrap(), MAGIC);
    assert_eq!(
        receiver.read_u32().await.unwrap(),
        NETWORK_MESSAGE_VERSION
    );
    let length = receiver.read_u64().await.unwrap();
    assert_eq!(length as usize, fixture(OLD_NOTIFY_JOB_LISTENERS).len());
    let mut payload = vec![0; length as usize];
    receiver.read_exact(&mut payload).await.unwrap();
    assert_eq!(payload, fixture(OLD_NOTIFY_JOB_LISTENERS));
}

#[tokio::test]
async fn unknown_message_fails_after_framing_before_processing() {
    let (mut sender, mut receiver) = connected_streams().await;
    write_frame(&mut sender, NETWORK_MESSAGE_VERSION, &unknown_message_fixture()).await;

    let error = NetworkMessage::receive(&mut receiver).await.unwrap_err();
    let message = error.message.to_ascii_lowercase();
    assert!(
        message.contains("unknown") || message.contains("variant"),
        "unexpected unknown-message error: {}",
        error.message
    );
}

#[tokio::test]
async fn mixed_protocol_version_fails_fast_before_payload_processing() {
    let (mut sender, mut receiver) = connected_streams().await;
    write_frame(&mut sender, NETWORK_MESSAGE_VERSION + 1, b"not flexbuffers").await;

    let error = NetworkMessage::receive(&mut receiver).await.unwrap_err();
    assert!(error
        .message
        .contains("Network protocol version mismatch"));
}
