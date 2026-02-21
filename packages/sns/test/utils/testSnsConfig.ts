import type { S3ClientConfig } from '@aws-sdk/client-s3'
import type { SNSClientConfig } from '@aws-sdk/client-sns'
import type { SQSClientConfig } from '@aws-sdk/client-sqs'
import { createLocalhostHandler } from 'fauxqs'

import { getPort } from './fauxqsInstance.ts'

const isLocalstack = process.env.QUEUE_BACKEND === 'localstack'
const port = getPort()

export const TEST_AWS_CONFIG: SNSClientConfig & SQSClientConfig = {
  endpoint: `http://localhost:${port}`,
  region: 'eu-west-1',
  credentials: {
    accessKeyId: 'access',
    secretAccessKey: 'secret',
  },
}

let s3Config: S3ClientConfig

if (isLocalstack) {
  s3Config = {
    endpoint: `http://s3.localhost.localstack.cloud:${port}`,
    region: 'eu-west-1',
    credentials: {
      accessKeyId: 'access',
      secretAccessKey: 'secret',
    },
  }
} else {
  s3Config = {
    endpoint: `http://s3.localhost:${port}`,
    region: 'eu-west-1',
    credentials: {
      accessKeyId: 'access',
      secretAccessKey: 'secret',
    },
    requestHandler: createLocalhostHandler(),
  }
}

export const TEST_S3_CONFIG: S3ClientConfig = s3Config
