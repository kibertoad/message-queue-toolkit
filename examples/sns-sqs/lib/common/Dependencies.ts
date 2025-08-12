import { SNSClient, type SNSClientConfig } from '@aws-sdk/client-sns'
import { SQSClient } from '@aws-sdk/client-sqs'
import { STSClient } from '@aws-sdk/client-sts'
import { SnsConsumerErrorResolver } from '@message-queue-toolkit/sns'
import pino from 'pino'
import { UserConsumer } from './UserConsumer.ts'

export const TEST_AWS_CONFIG: SNSClientConfig = {
  endpoint: 'http://s3.localhost.localstack.cloud:4566',
  region: 'eu-west-1',
  credentials: {
    accessKeyId: 'access',
    secretAccessKey: 'secret',
  },
}

export const sqsClient = new SQSClient(TEST_AWS_CONFIG)
export const snsClient = new SNSClient(TEST_AWS_CONFIG)
export const stsClient = new STSClient(TEST_AWS_CONFIG)

export const errorReporter = { report: () => {} }
export const logger = pino()
export const transactionObservabilityManager = {
  start: () => {},
  startWithGroup: () => {},
  stop: () => {},
  addCustomAttributes: () => {},
}

export const userConsumer = new UserConsumer({
  errorReporter,
  logger,
  transactionObservabilityManager,
  consumerErrorResolver: new SnsConsumerErrorResolver(),
  sqsClient,
  snsClient,
  stsClient,
})
