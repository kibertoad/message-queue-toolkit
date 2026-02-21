import type { S3 } from '@aws-sdk/client-s3'
import type { SNSClient } from '@aws-sdk/client-sns'
import type { SQSClient } from '@aws-sdk/client-sqs'
import type { STSClient } from '@aws-sdk/client-sts'
import { deleteQueue } from '@message-queue-toolkit/sqs'
import type { FauxqsServer } from 'fauxqs'

import { assertBucket, emptyBucket } from './s3Utils.ts'

export class TestAwsResourceAdmin {
  private server: FauxqsServer | undefined
  private sqsClient: SQSClient
  private s3?: S3
  private snsClient: SNSClient
  private stsClient: STSClient

  constructor(opts: {
    server: FauxqsServer | undefined
    sqsClient: SQSClient
    s3?: S3
    snsClient: SNSClient
    stsClient: STSClient
  }) {
    this.server = opts.server
    this.sqsClient = opts.sqsClient
    this.s3 = opts.s3
    this.snsClient = opts.snsClient
    this.stsClient = opts.stsClient
  }

  async purge(...queueNames: string[]) {
    if (this.server) {
      this.server.reset()
      for (const name of queueNames) {
        this.server.deleteQueue(name)
      }
      return
    }
    for (const name of queueNames) {
      await deleteQueue(this.sqsClient, name)
    }
  }

  async createBucket(name: string) {
    if (this.server) {
      this.server.createBucket(name)
      return
    }
    return await assertBucket(this.s3!, name)
  }

  async emptyBucket(name: string) {
    if (this.server) {
      this.server.emptyBucket(name)
      return
    }
    return await emptyBucket(this.s3!, name)
  }
}
