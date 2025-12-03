import { randomUUID } from 'node:crypto'
import type { Readable } from 'node:stream'

import type { Bucket, Storage } from '@google-cloud/storage'
import type { PayloadStoreTypes, SerializedPayload } from '@message-queue-toolkit/core'

export type GCSAwareDependencies = {
  gcsStorage?: Storage
}

export type MessageQueuePayloadOffloadingConfig = {
  gcsPayloadOffloadingBucket?: string
  messageSizeThreshold: number
}

export function resolvePayloadStoreConfig(
  dependencies: GCSAwareDependencies,
  config?: MessageQueuePayloadOffloadingConfig,
) {
  if (!config?.gcsPayloadOffloadingBucket) return undefined
  if (!dependencies.gcsStorage) {
    throw new Error('Google Cloud Storage client is required for payload offloading')
  }

  return {
    store: new GCSPayloadStore(
      { gcsStorage: dependencies.gcsStorage },
      { bucketName: config.gcsPayloadOffloadingBucket },
    ),
    messageSizeThreshold: config.messageSizeThreshold,
  }
}

export type GCSPayloadStoreDependencies = {
  gcsStorage: Storage
}

export type GCSPayloadStoreConfiguration = {
  bucketName: string
  keyPrefix?: string
}

export class GCSPayloadStore implements PayloadStoreTypes {
  private readonly storage: Storage
  private readonly bucket: Bucket
  private readonly config: GCSPayloadStoreConfiguration

  constructor({ gcsStorage }: GCSPayloadStoreDependencies, config: GCSPayloadStoreConfiguration) {
    this.storage = gcsStorage
    this.bucket = this.storage.bucket(config.bucketName)
    this.config = config
  }

  async storePayload(payload: SerializedPayload): Promise<string> {
    const id = randomUUID()
    const key = this.config?.keyPrefix?.length ? `${this.config.keyPrefix}/${id}` : id

    const file = this.bucket.file(key)

    // Handle both string and stream payloads
    if (typeof payload.value === 'string') {
      await file.save(payload.value, {
        metadata: {
          contentLength: payload.size,
        },
      })
    } else {
      // Stream
      await new Promise<void>((resolve, reject) => {
        const writeStream = file.createWriteStream({
          metadata: {
            contentLength: payload.size,
          },
        })

        ;(payload.value as Readable)
          .pipe(writeStream)
          .on('finish', () => resolve())
          .on('error', reject)
      })
    }

    return key
  }

  async retrievePayload(key: string): Promise<Readable | null> {
    try {
      const file = this.bucket.file(key)
      const [exists] = await file.exists()

      if (!exists) {
        return null
      }

      return file.createReadStream()
    } catch (error) {
      // Check if it's a not-found error (404)
      // biome-ignore lint/suspicious/noExplicitAny: error type is unknown
      if ((error as any)?.code === 404) {
        return null
      }
      throw error
    }
  }

  async deletePayload(key: string): Promise<void> {
    try {
      const file = this.bucket.file(key)
      await file.delete({ ignoreNotFound: true })
    } catch (error) {
      // Gracefully handle 404 errors (file already deleted or never existed)
      // biome-ignore lint/suspicious/noExplicitAny: error type is unknown
      if ((error as any)?.code === 404) {
        return
      }
      // Re-throw other errors
      throw error
    }
  }
}
