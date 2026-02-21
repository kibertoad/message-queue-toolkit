import type { S3ClientConfig } from '@aws-sdk/client-s3'
import { createLocalhostHandler } from 'fauxqs'

import { getFauxqsPort } from './fauxqsInstance.ts'

export function getTestS3Config(): S3ClientConfig {
  const port = getFauxqsPort()
  return {
    endpoint: `http://s3.localhost:${port}`,
    region: 'eu-west-1',
    credentials: {
      accessKeyId: 'access',
      secretAccessKey: 'secret',
    },
    requestHandler: createLocalhostHandler(),
  }
}
