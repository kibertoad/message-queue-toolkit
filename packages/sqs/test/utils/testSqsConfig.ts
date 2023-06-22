import type {SQSClientConfig } from '@aws-sdk/client-sqs'

export const TEST_SQS_CONFIG: SQSClientConfig = {
  endpoint: 'http://s3.localhost.localstack.cloud:4566',
  region: 'eu-west-1',
  credentials: {
    accessKeyId: 'access',
    secretAccessKey: 'secret',
  },
}
