import type { S3ClientConfig } from '@aws-sdk/client-s3'

export const TEST_AWS_CONFIG: S3ClientConfig = {
  endpoint: 'http://s3.localhost.localstack.cloud:4566',
  region: 'eu-west-1',
  credentials: {
    accessKeyId: 'access',
    secretAccessKey: 'secret',
  },
}
