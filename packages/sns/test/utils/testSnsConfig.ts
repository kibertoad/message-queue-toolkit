import type { S3ClientConfig } from '@aws-sdk/client-s3'
import type { SNSClientConfig } from '@aws-sdk/client-sns'
import type { SQSClientConfig } from '@aws-sdk/client-sqs'

export const TEST_AWS_CONFIG: SNSClientConfig & SQSClientConfig & S3ClientConfig = {
  endpoint: 'http://s3.localhost.localstack.cloud:4566',
  region: 'eu-west-1',
  credentials: {
    accessKeyId: 'access',
    secretAccessKey: 'secret',
  },
}
