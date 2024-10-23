import type { STSClient } from '@aws-sdk/client-sts'
import { beforeAll, describe, expect, it } from 'vitest'
import { registerDependencies } from '../../test/utils/testContext'
import { buildTopicArn } from './stsUtils'

describe('stsUtils', () => {
  let stsClient: STSClient

  beforeAll(async () => {
    const diContainer = await registerDependencies({}, false)
    stsClient = diContainer.cradle.stsClient
  })

  describe('buildTopicArn', () => {
    it('build ARN for topic', async () => {
      const topicName = 'my-test-topic'
      await expect(buildTopicArn(stsClient, topicName)).resolves.toMatchInlineSnapshot(
        `"arn:aws:sns:eu-west-1:000000000000:my-test-topic"`,
      )
    })
  })
})
