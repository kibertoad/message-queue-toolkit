import type { S3 } from '@aws-sdk/client-s3'
import type { SNSClient } from '@aws-sdk/client-sns'
import type { SQSClient } from '@aws-sdk/client-sqs'
import type { STSClient } from '@aws-sdk/client-sts'
import { assertQueue, deleteQueue } from '@message-queue-toolkit/sqs'
import type { FauxqsServer } from 'fauxqs'
import { assertTopic, deleteTopic } from '../../lib/utils/snsUtils.ts'
import { assertBucket, emptyBuckets } from './s3Utils.ts'

export class TestAwsResourceAdmin {
  private server: FauxqsServer | undefined
  private sqsClient: SQSClient
  private s3?: S3
  private snsClient: SNSClient
  private stsClient: STSClient
  private region: string

  constructor(opts: {
    server: FauxqsServer | undefined
    sqsClient: SQSClient
    s3?: S3
    snsClient: SNSClient
    stsClient: STSClient
    region: string
  }) {
    this.server = opts.server
    this.sqsClient = opts.sqsClient
    this.s3 = opts.s3
    this.snsClient = opts.snsClient
    this.stsClient = opts.stsClient
    this.region = opts.region
  }

  async createQueue(
    name: string,
    opts?: { attributes?: Record<string, string>; tags?: Record<string, string> },
  ) {
    if (this.server) {
      return this.server.createQueue(name, {
        region: this.region,
        attributes: opts?.attributes,
        tags: opts?.tags,
      })
    }
    return await assertQueue(this.sqsClient, {
      QueueName: name,
      Attributes: opts?.attributes,
      tags: opts?.tags,
    })
  }

  async deleteQueues(...queueNames: string[]) {
    if (this.server) {
      for (const name of queueNames) {
        this.server.deleteQueue(name, { region: this.region })
      }
      return
    }
    for (const name of queueNames) {
      await deleteQueue(this.sqsClient, name)
    }
  }

  async createTopic(name: string) {
    if (this.server) {
      const { topicArn } = this.server.createTopic(name, { region: this.region })
      return topicArn
    }
    return await assertTopic(this.snsClient, this.stsClient, { Name: name })
  }

  async deleteTopics(...topicNames: string[]) {
    if (this.server) {
      for (const name of topicNames) {
        this.server.deleteTopic(name, { region: this.region })
      }
      return
    }
    for (const name of topicNames) {
      await deleteTopic(this.snsClient, this.stsClient, name)
    }
  }

  async createBucket(name: string) {
    if (this.server) {
      this.server.createBucket(name)
      return
    }
    return await assertBucket(this.s3!, name)
  }

  async emptyBuckets(...names: string[]) {
    if (this.server) {
      for (const name of names) {
        this.server.emptyBucket(name)
      }
      return
    }
    return await emptyBuckets(this.s3!, ...names)
  }
}
