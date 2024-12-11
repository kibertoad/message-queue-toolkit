import type { SNSClient } from '@aws-sdk/client-sns'
import type { STSClient } from '@aws-sdk/client-sts'
import { beforeAll, beforeEach, describe, expect, it, vi } from 'vitest'
import { registerDependencies } from '../../test/utils/testContext'
import { assertTopic, deleteTopic } from './snsUtils'
import { buildTopicArn, clearCachedCallerIdentity } from './stsUtils'

describe('stsUtils', () => {
  let stsClient: STSClient
  let snsClient: SNSClient

  beforeAll(async () => {
    const diContainer = await registerDependencies({}, false)
    stsClient = diContainer.cradle.stsClient
    snsClient = diContainer.cradle.snsClient
  })

  describe('buildTopicArn', () => {
    const topicName = 'my-test-topic'

    beforeEach(async () => {
      await deleteTopic(snsClient, stsClient, topicName)
      clearCachedCallerIdentity()
    })

    it('build ARN for topic', async () => {
      const buildedTopicArn = await buildTopicArn(stsClient, topicName)
      expect(buildedTopicArn).toMatchInlineSnapshot(
        `"arn:aws:sns:eu-west-1:000000000000:my-test-topic"`,
      )

      // creating real topic to make sure arn is correct
      const topicArn = await assertTopic(snsClient, stsClient, { Name: topicName })
      expect(topicArn).toBe(buildedTopicArn)
    })

    it('should be able to handle parallel calls getting caller identity only once', async () => {
      const stsClientSpy = vi.spyOn(stsClient, 'send')

      const result = await Promise.all([
        buildTopicArn(stsClient, topicName),
        buildTopicArn(stsClient, topicName),
        buildTopicArn(stsClient, topicName),
      ])

      expect(result).toMatchInlineSnapshot(`
        [
          "arn:aws:sns:eu-west-1:000000000000:my-test-topic",
          "arn:aws:sns:eu-west-1:000000000000:my-test-topic",
          "arn:aws:sns:eu-west-1:000000000000:my-test-topic",
        ]
      `)
      expect(stsClientSpy).toHaveBeenCalledOnce()
    })
  })
})
