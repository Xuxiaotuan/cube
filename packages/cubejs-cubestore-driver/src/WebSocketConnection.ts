import WebSocket from 'ws';
import * as flatbuffers from 'flatbuffers';
import { v4 as uuidv4 } from 'uuid';
import { InlineTable } from '@cubejs-backend/base-driver';
import { getEnv, getProcessUid } from '@cubejs-backend/shared';
import { parseCubestoreResultMessage } from '@cubejs-backend/native';
import { ConnectionError, QueryError } from './errors';
import {
  BinaryValue,
  BoolValue,
  Float64Value,
  HttpCommand,
  HttpError,
  HttpMessage,
  HttpParameter,
  HttpParameterValue,
  HttpQuery,
  HttpTable,
  Int64Value,
  NullValue,
  QueryResultFormat,
  StringValue,
} from '../codegen';

interface SentMessage {
  resolve: (value: any) => void;
  reject: (reason?: any) => void;
  buffer: Uint8Array;
  replaySafe: boolean;
}

export class MutationUnknownError extends ConnectionError {
  public constructor(message: string, cause?: Error) {
    super(message, cause);
    this.name = 'MutationUnknownError';
    (this as any).code = 'MUTATION_UNKNOWN';
  }
}

export type QueryParameter = null | boolean | number | string | Buffer;

export type WebSocketQueryOptions = {
  inlineTables?: InlineTable[];
  queryTracingObj?: any;
  responseFormat: QueryResultFormat;
  replaySafe?: boolean;
};

interface CubeStoreWebSocket extends WebSocket {
  readyPromise: Promise<CubeStoreWebSocket>;
  lastHeartBeat: Date;
  sentMessages: Record<number, SentMessage>;
  sendAsync: (message: Uint8Array) => Promise<void>;
}

export class WebSocketConnection {
  protected messageCounter: number;

  protected readonly maxConnectRetries: number;

  protected readonly noHeartBeatTimeout: number;

  protected currentConnectionTry: number;

  protected webSocket: CubeStoreWebSocket | null = null;

  private readonly url: string;

  private readonly connectionId: string;

  private cubeStoreVersion: string | null = null;

  public constructor(url: string) {
    this.url = url;
    this.messageCounter = 1;
    this.maxConnectRetries = getEnv('cubeStoreMaxConnectRetries');
    this.noHeartBeatTimeout = getEnv('cubeStoreNoHeartBeatTimeout');
    this.currentConnectionTry = 0;
    this.connectionId = uuidv4();
  }

  protected async initWebSocket(): Promise<CubeStoreWebSocket> {
    if (!this.webSocket) {
      const headers: Record<string, string> = {};
      headers['x-process-id'] = getProcessUid();

      const webSocket = new WebSocket(this.url, { headers }) as CubeStoreWebSocket;
      webSocket.on('upgrade', (response: any) => {
        this.cubeStoreVersion = response.headers['x-cubestore-version'] || null;
      });

      webSocket.readyPromise = new Promise<CubeStoreWebSocket>((resolve, reject) => {
        webSocket.lastHeartBeat = new Date();
        const pingInterval = setInterval(() => {
          if (webSocket.readyState === WebSocket.OPEN) {
            webSocket.ping();
          }

          if (new Date().getTime() - webSocket.lastHeartBeat.getTime() > this.noHeartBeatTimeout * 1000) {
            webSocket.close();
          }
        }, 5000);

        webSocket.sendAsync = async (message: Uint8Array) => new Promise<void>((resolveSend, rejectSend) => {
          // If socket is closing this message should be resent
          if (webSocket.readyState === WebSocket.OPEN) {
            webSocket.send(message, (err) => {
              if (err) {
                rejectSend(new ConnectionError(
                  `CubeStore connection error: ${err.message}`,
                  err
                ));
              } else {
                resolveSend();
              }
            });
          }
        });
        webSocket.on('open', () => resolve(webSocket));
        webSocket.on('error', (err) => {
          this.currentConnectionTry += 1;

          if (this.currentConnectionTry < this.maxConnectRetries) {
            setTimeout(async () => {
              resolve(this.initWebSocket());
            }, this.retryWaitTime());
          } else {
            reject(new ConnectionError(
              `CubeStore connection failed after ${this.maxConnectRetries} retries: ${err.message}`,
              err
            ));
          }

          if (webSocket === this.webSocket) {
            this.webSocket = null;
          }
        });
        webSocket.on('pong', () => {
          if (webSocket === this.webSocket) {
            this.currentConnectionTry = 0;
          }
          webSocket.lastHeartBeat = new Date();
        });
        webSocket.on('close', () => {
          clearInterval(pingInterval);

          // Settle unknown writes without waiting for a new leader connection.
          // A failed reconnect must never downgrade UNKNOWN to a generic error.
          for (const key of Object.keys(webSocket.sentMessages)) {
            const pending = webSocket.sentMessages[key];
            if (!pending.replaySafe) {
              delete webSocket.sentMessages[key];
              pending.reject(new MutationUnknownError('CubeStore connection closed after sending a non-idempotent request; mutation outcome is unknown'));
            }
          }

          const pendingMessageKeys = Object.keys(webSocket.sentMessages);
          if (pendingMessageKeys.length) {
            setTimeout(async () => {
              try {
                const nextWebSocket = await this.initWebSocket();
                const replaySafeEntries: [string, SentMessage][] = [];
                // eslint-disable-next-line no-restricted-syntax
                for (const key of pendingMessageKeys) {
                  const pending = webSocket.sentMessages[key];
                  if (pending?.replaySafe) {
                    replaySafeEntries.push([key, pending]);
                  }
                }

                // eslint-disable-next-line no-restricted-syntax
                for (const [key, pending] of replaySafeEntries) {
                  nextWebSocket.sentMessages[key] = pending;
                  await nextWebSocket.sendAsync(pending.buffer);
                }

                // eslint-disable-next-line no-restricted-syntax
                for (const key of pendingMessageKeys) {
                  const pending = webSocket.sentMessages[key];
                  if (pending && !pending.replaySafe) {
                    pending.reject(new MutationUnknownError(
                      'CubeStore connection closed after sending a non-idempotent request; mutation outcome is unknown',
                    ));
                  }
                }
              } catch (e) {
                // eslint-disable-next-line no-restricted-syntax
                for (const key of pendingMessageKeys) {
                  webSocket.sentMessages[key].reject(e);
                }
              }
            }, this.retryWaitTime());
          }

          if (webSocket === this.webSocket) {
            this.webSocket = null;
          }
        });
        webSocket.on('message', async (msg: Buffer) => {
          const buf = new flatbuffers.ByteBuffer(msg);
          const httpMessage = HttpMessage.getRootAsHttpMessage(buf);

          const resolver = webSocket.sentMessages[httpMessage.messageId()];
          if (!resolver) {
            throw new QueryError(`Cube Store missed message id: ${httpMessage.messageId()}`);
          }

          delete webSocket.sentMessages[httpMessage.messageId()];

          if (httpMessage.commandType() === HttpCommand.HttpError) {
            const message = `${httpMessage.command(new HttpError())?.error()}`;
            const normalizedMessage = message.toLowerCase();
            if (normalizedMessage.includes('wrongconnection') || normalizedMessage.includes('wrong connection')) {
              resolver.reject(new ConnectionError(`CubeStore connection error: ${message}`));
            } else {
              resolver.reject(new QueryError(message));
            }
            return;
          }

          try {
            const nativeResMsg = await parseCubestoreResultMessage(msg);
            resolver.resolve(nativeResMsg);
          } catch (e) {
            resolver.reject(e);
          }
        });
      });

      webSocket.sentMessages = {};
      this.webSocket = webSocket;
    }

    return this.webSocket!.readyPromise;
  }

  private retryWaitTime() {
    return 1000 * (this.currentConnectionTry + 1);
  }

  protected isReplaySafeQuery(query: string): boolean {
    let normalized = `${query}`.replace(/^\uFEFF/, '').trimStart();

    // eslint-disable-next-line no-constant-condition
    while (true) {
      const blockMatch = normalized.match(/^\/\*[\s\S]*?\*\//);
      const lineMatch = normalized.match(/^--.*?(?:\r\n|\r|\n|$)/);
      const shellMatch = normalized.match(/^#.*?(?:\r\n|\r|\n|$)/);

      if (blockMatch) {
        normalized = normalized.slice(blockMatch[0].length).trimStart();
        // eslint-disable-next-line no-continue
        continue;
      }

      if (lineMatch) {
        normalized = normalized.slice(lineMatch[0].length).trimStart();
        // eslint-disable-next-line no-continue
        continue;
      }

      if (shellMatch) {
        normalized = normalized.slice(shellMatch[0].length).trimStart();
        // eslint-disable-next-line no-continue
        continue;
      }

      break;
    }

    const normalizedUpper = normalized.toUpperCase();
    if (!normalized) {
      return false;
    }

    const head = normalizedUpper.split(/\s+/)[0];
    if (/^CACHE\s+(GET|KEYS)\s/.test(normalizedUpper)) return true;
    const replaySafeHeads = ['SELECT', 'SHOW', 'DESCRIBE', 'EXPLAIN', 'PRAGMA'];
    return replaySafeHeads.includes(head);
  }

  private async sendMessage(messageId: number, buffer: Uint8Array, replaySafe = false): Promise<any> {
    let socket: CubeStoreWebSocket;
    try {
      socket = await this.initWebSocket();
    } catch (error: any) {
      error.code = 'MUTATION_NOT_DISPATCHED';
      throw error;
    }
    return new Promise((resolve, reject) => {
      socket.sentMessages[messageId] = {
        resolve,
        reject,
        buffer,
        replaySafe,
      };
      if (socket.readyState === WebSocket.OPEN) {
        socket.send(buffer, (err) => {
          if (err) {
            delete socket.sentMessages[messageId];
            reject(new MutationUnknownError(
              `CubeStore connection error: ${err.message}`,
              err
            ));
          }
        });
      } else {
        delete socket.sentMessages[messageId];
        const error = new ConnectionError('CubeStore connection closed before request could be sent');
        (error as any).code = 'MUTATION_NOT_DISPATCHED';
        reject(error);
      }
    });
  }

  protected serializeParameter(builder: flatbuffers.Builder, parameter: unknown) {
    if (parameter === null || parameter === undefined) {
      const httpParameterValueOffset = NullValue.createNullValue(builder);

      return HttpParameter.createHttpParameter(
        builder,
        HttpParameterValue.NullValue,
        httpParameterValueOffset
      );
    }

    switch (typeof parameter) {
      case 'object':
      {
        if (Buffer.isBuffer(parameter)) {
          const valueOffset = BinaryValue.createVVector(builder, parameter);
          const httpParameterValueOffset = BinaryValue.createBinaryValue(builder, valueOffset);

          return HttpParameter.createHttpParameter(
            builder,
            HttpParameterValue.BinaryValue,
            httpParameterValueOffset
          );
        } else {
          throw new Error('Parameter with type: object is not supported');
        }
      }
      case 'boolean':
      {
        const httpParameterValueOffset = BoolValue.createBoolValue(
          builder,
          parameter
        );

        return HttpParameter.createHttpParameter(
          builder,
          HttpParameterValue.BoolValue,
          httpParameterValueOffset
        );
      }
      case 'number':
      {
        if (Number.isInteger(parameter)) {
          const httpParameterValueOffset = Int64Value.createInt64Value(builder, BigInt(parameter));

          return HttpParameter.createHttpParameter(
            builder,
            HttpParameterValue.Int64Value,
            httpParameterValueOffset
          );
        } else {
          const httpParameterValueOffset = Float64Value.createFloat64Value(builder, parameter);

          return HttpParameter.createHttpParameter(
            builder,
            HttpParameterValue.Float64Value,
            httpParameterValueOffset
          );
        }
      }
      case 'string':
      {
        const valueOffset = builder.createString(parameter);
        const httpParameterValueOffset = StringValue.createStringValue(builder, valueOffset);

        return HttpParameter.createHttpParameter(
          builder,
          HttpParameterValue.StringValue,
          httpParameterValueOffset
        );
      }
      default:
        throw new Error(`Parameter with type: ${typeof parameter} is not supported`);
    }
  }

  public async query(query: string, parameters: QueryParameter[], options: WebSocketQueryOptions): Promise<any[]> {
    const { inlineTables, queryTracingObj, responseFormat } = options;
    // A caller-provided flag cannot turn a mutation into a safe websocket replay.
    // Mutation retries are coordinated by the Driver's idempotency state machine
    // after it observes a completed authoritative result.
    const replaySafe = (options.replaySafe ?? this.isReplaySafeQuery(query)) && this.isReplaySafeQuery(query);

    const builder = new flatbuffers.Builder(1024);
    const queryOffset = builder.createString(query);

    let traceObjOffset: number | null = null;
    if (queryTracingObj) {
      traceObjOffset = builder.createString(JSON.stringify(queryTracingObj));
    }

    let inlineTablesOffset: number | null = null;
    if (inlineTables && inlineTables.length > 0) {
      const inlineTableOffsets: number[] = [];
      for (const table of inlineTables) {
        const nameOffset = builder.createString(table.name);
        const columnOffsets: number[] = [];
        for (const column of table.columns) {
          const columnOffset = builder.createString(column.name);
          columnOffsets.push(columnOffset);
        }
        const columnsOffset = HttpTable.createColumnsVector(builder, columnOffsets);
        const typeOffsets: number[] = [];
        for (const column of table.columns) {
          const typeOffset = builder.createString(column.type);
          typeOffsets.push(typeOffset);
        }
        const typesOffset = HttpTable.createColumnsVector(builder, typeOffsets);
        const csvRowsOffset = builder.createString(table.csvRows);
        HttpTable.startHttpTable(builder);
        HttpTable.addName(builder, nameOffset);
        HttpTable.addColumns(builder, columnsOffset);
        HttpTable.addTypes(builder, typesOffset);
        HttpTable.addCsvRows(builder, csvRowsOffset);
        const inlineTableOffset = HttpTable.endHttpTable(builder);
        inlineTableOffsets.push(inlineTableOffset);
      }
      inlineTablesOffset = HttpQuery.createInlineTablesVector(builder, inlineTableOffsets);
    }

    let parametersOffset: flatbuffers.Offset | null = null;
    if (parameters.length > 0) {
      const httpParameterValues: flatbuffers.Offset[] = [];

      for (const parameter of parameters) {
        httpParameterValues.push(this.serializeParameter(builder, parameter));
      }

      parametersOffset = HttpQuery.createParametersVector(
        builder,
        httpParameterValues
      );
    }

    HttpQuery.startHttpQuery(builder);
    HttpQuery.addQuery(builder, queryOffset);

    if (traceObjOffset) {
      HttpQuery.addTraceObj(builder, traceObjOffset);
    }

    if (inlineTablesOffset) {
      HttpQuery.addInlineTables(builder, inlineTablesOffset);
    }

    if (parametersOffset) {
      HttpQuery.addParameters(builder, parametersOffset);
    }

    HttpQuery.addResponseFormat(builder, responseFormat);

    const httpQueryOffset = HttpQuery.endHttpQuery(builder);
    const messageId = this.messageCounter++;
    const connectionIdOffset = builder.createString(this.connectionId);
    const message = HttpMessage.createHttpMessage(builder, messageId, HttpCommand.HttpQuery, httpQueryOffset, connectionIdOffset);
    builder.finish(message);
    return this.sendMessage(
      messageId,
      builder.asUint8Array(),
      replaySafe,
    );
  }

  public async getCubeStoreVersion(): Promise<string> {
    if (this.webSocket) {
      await this.webSocket.readyPromise;
    }

    return this.cubeStoreVersion ?? '0.0.0';
  }

  public close() {
    const webSocket = this.webSocket;
    this.webSocket = null;
    if (webSocket) {
      webSocket.close();
    }
  }
}
