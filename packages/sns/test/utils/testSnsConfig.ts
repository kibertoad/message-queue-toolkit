import type { SNSClientConfig } from '@aws-sdk/client-sns'

export const TEST_AWS_CONFIG: SNSClientConfig = {
  endpoint: 'http://s3.localhost.localstack.cloud:4566',
  region: 'eu-west-1',
  credentials: {
    accessKeyId: 'access',
    secretAccessKey: 'secret',
  },
}
