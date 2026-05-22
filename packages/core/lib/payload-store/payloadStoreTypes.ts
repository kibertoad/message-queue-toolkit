import type { Readable } from 'node:stream'

export interface PayloadStore {
  /** Store the payload and return a key that can be used to retrieve it later. */
  storePayload(payload: SerializedPayload): Promise<string>

  /** Retrieve the previously stored payload. */
  retrievePayload(key: string): Promise<Readable | null>
}

export type SerializedPayload = {
  value: string | Readable
  size: number
}

export type Destroyable<T> = T & {
  destroy(): Promise<void>
}

export function isDestroyable(value: unknown): value is Destroyable<unknown> {
  return typeof value === 'object' && value !== null && 'destroy' in value
}

export interface PayloadSerializer {
  serialize(payload: unknown): Promise<SerializedPayload | Destroyable<SerializedPayload>>
}

/**
 * Single-store configuration (simple mode).
 * Use this when you have only one payload store.
 */
export type SinglePayloadStoreConfig = {
  /**
   * Wire-body size threshold in bytes. Messages whose wire body exceeds this value
   * are offloaded to the store; smaller messages are sent inline.
   *
   * **What counts as "wire body size"** depends on whether a codec is active:
   * - Without codec: the UTF-8 byte length of `JSON.stringify(message)`.
   * - With codec: the byte length of the codec envelope
   *   (`{"__mqtCodec":"zstd","__mqtData":"<base64>"}`) — i.e. the compressed
   *   payload after base64 encoding and JSON framing (~4/3 of the compressed size).
   *
   * Because codec reduces payload size before the threshold is applied, enabling
   * codec effectively raises the bar for offloading: a 500 KB message that
   * compresses to 100 KB will not trigger a 200 KB threshold.
   * Size your threshold accordingly, or set it to the protocol's hard limit
   * (e.g. `SQS_MESSAGE_MAX_SIZE`) to offload only when strictly necessary.
   */
  messageSizeThreshold: number

  /** The store to use for storing the payload. */
  store: PayloadStore

  /**
   * Identifier for this store (used in offloaded payload messages).
   * This name is embedded in the message payload to identify which store holds the data.
   */
  storeName: string

  /** The serializer to use for serializing the payload. */
  serializer?: PayloadSerializer
}

/**
 * Multi-store configuration (advanced mode).
 * Use this when you need to support multiple payload stores (e.g., for migration).
 */
export type MultiPayloadStoreConfig<StoreNames extends string = string> = {
  /**
   * Wire-body size threshold in bytes. Messages whose wire body exceeds this value
   * are offloaded to the store; smaller messages are sent inline.
   *
   * **What counts as "wire body size"** depends on whether a codec is active:
   * - Without codec: the UTF-8 byte length of `JSON.stringify(message)`.
   * - With codec: the byte length of the codec envelope
   *   (`{"__mqtCodec":"zstd","__mqtData":"<base64>"}`) — i.e. the compressed
   *   payload after base64 encoding and JSON framing (~4/3 of the compressed size).
   *
   * Because codec reduces payload size before the threshold is applied, enabling
   * codec effectively raises the bar for offloading: a 500 KB message that
   * compresses to 100 KB will not trigger a 200 KB threshold.
   * Size your threshold accordingly, or set it to the protocol's hard limit
   * (e.g. `SQS_MESSAGE_MAX_SIZE`) to offload only when strictly necessary.
   */
  messageSizeThreshold: number

  /** Map of store identifiers to store instances. */
  stores: Record<StoreNames, PayloadStore>

  /**
   * Store identifier to use for outgoing messages.
   * Must be a key from the stores map (enforced at compile time).
   */
  outgoingStore: NoInfer<StoreNames>

  /**
   * Optional: Default store identifier to use when retrieving messages that only have
   * the legacy offloadedPayloadPointer field (without payloadRef).
   * If not specified, will throw an error when encountering legacy format.
   * Must be a key from the stores map if provided (enforced at compile time).
   */
  defaultIncomingStore?: NoInfer<StoreNames>

  /** The serializer to use for serializing the payload. */
  serializer?: PayloadSerializer
}

/**
 * Payload store configuration - supports both single-store and multi-store modes.
 */
export type PayloadStoreConfig = SinglePayloadStoreConfig | MultiPayloadStoreConfig

/**
 * Type guard to check if config is multi-store configuration.
 */
export function isMultiPayloadStoreConfig(
  config: PayloadStoreConfig,
): config is MultiPayloadStoreConfig<string> {
  return 'stores' in config && 'outgoingStore' in config
}

/**
 * Helper function to create a multi-store config with compile-time validation.
 * TypeScript will infer store names from the `stores` object and ensure
 * `outgoingStore` and `defaultIncomingStore` are valid keys.
 *
 * @example
 * ```typescript
 * const config = createMultiStoreConfig({
 *   messageSizeThreshold: 256 * 1024,
 *   stores: {
 *     'store-a': storeA,
 *     'store-b': storeB,
 *   },
 *   outgoingStore: 'store-a', // Valid
 *   // outgoingStore: 'store-c', // Compile error
 * })
 * ```
 */
export function createMultiStoreConfig<StoreNames extends string>(
  config: MultiPayloadStoreConfig<StoreNames>,
): MultiPayloadStoreConfig<StoreNames> {
  return config
}
