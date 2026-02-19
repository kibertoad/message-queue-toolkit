import type { S3ClientConfig } from '@aws-sdk/client-s3'
import type { SNSClientConfig } from '@aws-sdk/client-sns'
import type { SQSClientConfig } from '@aws-sdk/client-sqs'
import { createLocalhostHandler } from 'fauxqs'

export const TEST_AWS_CONFIG: SNSClientConfig & SQSClientConfig = {
  endpoint: 'http://localhost:4566',
  region: 'eu-west-1',
  credentials: {
    accessKeyId: 'access',
    secretAccessKey: 'secret',
  },
}

export const TEST_S3_CONFIG: S3ClientConfig = {
  endpoint: 'http://s3.localhost:4566',
  region: 'eu-west-1',
  credentials: {
    accessKeyId: 'access',
    secretAccessKey: 'secret',
  },
  requestHandler: createLocalhostHandler(),
}
