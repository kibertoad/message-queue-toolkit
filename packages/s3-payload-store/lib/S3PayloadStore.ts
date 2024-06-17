import { randomUUID } from 'node:crypto'
import type { Readable } from 'node:stream'

import type { S3 } from '@aws-sdk/client-s3'
import { NoSuchKey } from '@aws-sdk/client-s3'
import type { PayloadStore } from '@message-queue-toolkit/core'

export type S3PayloadStoreDependencies = {
  s3: S3
}

export type S3PayloadStoreConfiguration = {
  bucketName: string
  keyPrefix?: string
}

export class S3PayloadStore implements PayloadStore {
  private s3: S3
  private readonly config: S3PayloadStoreConfiguration

  constructor({ s3 }: S3PayloadStoreDependencies, config: S3PayloadStoreConfiguration) {
    this.s3 = s3
    this.config = config
  }

  async storePayload(payload: Readable, payloadSize: number) {
    const id = randomUUID()
    const key = this.config?.keyPrefix?.length ? `${this.config.keyPrefix}/${id}` : id
    await this.s3.putObject({
      Bucket: this.config.bucketName,
      Key: key,
      Body: payload,
      ContentLength: payloadSize,
    })
    return key
  }

  async retrievePayload(key: string) {
    try {
      const result = await this.s3.getObject({
        Bucket: this.config.bucketName,
        Key: key,
      })
      return result.Body ? (result.Body as Readable) : null
    } catch (e) {
      if (e instanceof NoSuchKey) {
        return null
      }
      throw e
    }
  }

  async deletePayload(key: string) {
    await this.s3.deleteObject({
      Bucket: this.config.bucketName,
      Key: key,
    })
  }
}
