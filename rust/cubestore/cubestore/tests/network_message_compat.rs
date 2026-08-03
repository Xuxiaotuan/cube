use cubestore::cluster::message::NetworkMessage;
use cubestore::metastore::{MetaStoreRpcMethodCall, MetaStoreRpcMethodResult};
use serde::{Deserialize, Serialize};

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

#[test]
fn notify_job_listeners_preserves_master_encoding() {
    let bytes = "124e6f746966794a6f624c697374656e65727300131401";
    assert_eq!(encode(&NetworkMessage::NotifyJobListeners), fixture(bytes));
    assert!(matches!(decode(bytes), NetworkMessage::NotifyJobListeners));
}

#[test]
fn notify_job_listeners_success_preserves_master_encoding() {
    let bytes = "194e6f746966794a6f624c697374656e65727353756363657373001a1401";
    assert_eq!(
        encode(&NetworkMessage::NotifyJobListenersSuccess),
        fixture(bytes)
    );
    assert!(matches!(
        decode(bytes),
        NetworkMessage::NotifyJobListenersSuccess
    ));
}

#[test]
fn metastore_call_preserves_master_encoding() {
    let message = NetworkMessage::MetaStoreCall(MetaStoreRpcMethodCall::waitForCurrentSeqToSync);
    let bytes = "4d65746153746f726543616c6c001777616974466f7243757272656e74536571546f53796e630001280101011d14022401";
    assert_eq!(encode(&message), fixture(bytes));
    assert!(matches!(
        decode(bytes),
        NetworkMessage::MetaStoreCall(MetaStoreRpcMethodCall::waitForCurrentSeqToSync)
    ));
}

#[test]
fn metastore_call_result_preserves_master_encoding() {
    let message = NetworkMessage::MetaStoreCallResult(
        MetaStoreRpcMethodResult::waitForCurrentSeqToSync(Ok(())),
    );
    let bytes = "4d65746153746f726543616c6c526573756c740077616974466f7243757272656e74536571546f53796e63004f6b000104010101000001230101010724013e0101010724022401";
    assert_eq!(encode(&message), fixture(bytes));
    assert!(matches!(
        decode(bytes),
        NetworkMessage::MetaStoreCallResult(
            MetaStoreRpcMethodResult::waitForCurrentSeqToSync(Ok(()))
        )
    ));
}
