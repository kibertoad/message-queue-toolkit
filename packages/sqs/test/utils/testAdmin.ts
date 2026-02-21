import type { S3 } from '@aws-sdk/client-s3'
import type { SNSClient } from '@aws-sdk/client-sns'
import type { SQSClient } from '@aws-sdk/client-sqs'
import type { STSClient } from '@aws-sdk/client-sts'
import type { FauxqsServer } from 'fauxqs'

import { assertQueue, deleteQueue } from '../../lib/utils/sqsUtils.ts'
import { assertBucket, emptyBucket } from './s3Utils.ts'

export class TestAwsResourceAdmin {
  private server: FauxqsServer | undefined
  private sqsClient: SQSClient
  private s3?: S3
  private snsClient?: SNSClient
  private stsClient?: STSClient

  constructor(opts: {
    server: FauxqsServer | undefined
    sqsClient: SQSClient
    s3?: S3
    snsClient?: SNSClient
    stsClient?: STSClient
  }) {
    this.server = opts.server
    this.sqsClient = opts.sqsClient
    this.s3 = opts.s3
    this.snsClient = opts.snsClient
    this.stsClient = opts.stsClient
  }

  async createQueue(name: string, attrs?: Record<string, string>) {
    // if (this.server) {
    //   this.server.createQueue(name, { attributes: attrs })
    // }
    return await assertQueue(this.sqsClient, { QueueName: name, Attributes: attrs })
  }

  async deleteQueue(name: string) {
    return await deleteQueue(this.sqsClient, name)
  }

  async createBucket(name: string) {
    // if (this.server) {
    //   this.server.createBucket(name)
    //   return
    // }
    return await assertBucket(this.s3!, name)
  }

  async emptyBucket(name: string) {
    return await emptyBucket(this.s3!, name)
  }

  reset() {
    // if (this.server) {
    //   this.server.reset()
    // }
  }

  inspectQueue(name: string) {
    // return this.server?.inspectQueue(name)
    return undefined
  }
}
