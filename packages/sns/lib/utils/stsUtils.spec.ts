import type { SNSClient } from '@aws-sdk/client-sns'
import type { STSClient } from '@aws-sdk/client-sts'
import { beforeAll, beforeEach, describe, expect, it } from 'vitest'
import { registerDependencies } from '../../test/utils/testContext'
import { assertTopic, deleteTopic } from './snsUtils'

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
    })

    it('build ARN for topic', async () => {
      const expectedTopicArn = `arn:aws:sns:eu-west-1:000000000000:${topicName}`
      expect(expectedTopicArn).toMatchInlineSnapshot(
        `"arn:aws:sns:eu-west-1:000000000000:my-test-topic"`,
      )

      // creating real topic to make sure arn is correct
      const topicArn = await assertTopic(snsClient, stsClient, { Name: topicName })
      expect(topicArn).toBe(expectedTopicArn)
    })
  })
})
