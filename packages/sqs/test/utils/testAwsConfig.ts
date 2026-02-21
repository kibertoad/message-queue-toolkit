import type { S3ClientConfig } from '@aws-sdk/client-s3'
import type { SQSClientConfig } from '@aws-sdk/client-sqs'

const isLocalstack = process.env.QUEUE_BACKEND === 'localstack'

export const TEST_AWS_CONFIG: SQSClientConfig = {
  endpoint: 'http://localhost:4566',
  region: 'eu-west-1',
  credentials: {
    accessKeyId: 'access',
    secretAccessKey: 'secret',
  },
}

let s3Config: S3ClientConfig

if (isLocalstack) {
  s3Config = {
    endpoint: 'http://s3.localhost.localstack.cloud:4566',
    region: 'eu-west-1',
    credentials: {
      accessKeyId: 'access',
      secretAccessKey: 'secret',
    },
  }
} else {
  const { createLocalhostHandler } = await import('fauxqs')
  s3Config = {
    endpoint: 'http://s3.localhost:4566',
    region: 'eu-west-1',
    credentials: {
      accessKeyId: 'access',
      secretAccessKey: 'secret',
    },
    requestHandler: createLocalhostHandler(),
  }
}

export const TEST_S3_CONFIG: S3ClientConfig = s3Config
