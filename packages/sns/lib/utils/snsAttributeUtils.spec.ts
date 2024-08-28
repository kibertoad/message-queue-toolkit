import { describe, expect, it } from 'vitest'
import {
  PERMISSIONS_ADD_MESSAGE_SCHEMA,
  PERMISSIONS_REMOVE_MESSAGE_SCHEMA,
} from '../../test/consumers/userConsumerSchemas'

import { generateFilterAttributes, generateTopicSubscriptionPolicy } from './snsAttributeUtils'

describe('snsAttributeUtils', () => {
  describe('generateTopicSubscriptionPolicy', () => {
    it('resolves policy for both params', () => {
      const resolvedPolicy = generateTopicSubscriptionPolicy({
        topicArn: 'arn:aws:sns:eu-central-1:632374391739:test-sns-some-service',
        allowedSqsQueueUrlPrefix: 'arn:aws:sqs:eu-central-1:632374391739:test-sqs-*',
        allowedSourceOwner: '111111111111',
      })

      expect(resolvedPolicy).toBe(
        `{"Version":"2012-10-17","Id":"__default_policy_ID","Statement":[{"Sid":"AllowSQSSubscription","Effect":"Allow","Principal":{"AWS":"*"},"Action":["sns:Subscribe"],"Resource":"arn:aws:sns:eu-central-1:632374391739:test-sns-some-service","Condition":{"StringEquals":{"AWS:SourceOwner": "111111111111"},"StringLike":{"sns:Endpoint":"arn:aws:sqs:eu-central-1:632374391739:test-sqs-*"}}}]}`,
      )
    })

    it('resolves policy for one param', () => {
      const resolvedPolicy = generateTopicSubscriptionPolicy({
        topicArn: 'arn:aws:sns:eu-central-1:632374391739:test-sns-some-service',
        allowedSourceOwner: '111111111111',
      })

      expect(resolvedPolicy).toBe(
        `{"Version":"2012-10-17","Id":"__default_policy_ID","Statement":[{"Sid":"AllowSQSSubscription","Effect":"Allow","Principal":{"AWS":"*"},"Action":["sns:Subscribe"],"Resource":"arn:aws:sns:eu-central-1:632374391739:test-sns-some-service","Condition":{"StringEquals":{"AWS:SourceOwner": "111111111111"}}}]}`,
      )
    })

    it('resolves policy for zero params', () => {
      const resolvedPolicy = generateTopicSubscriptionPolicy({
        topicArn: 'arn:aws:sns:eu-central-1:632374391739:test-sns-some-service',
      })

      expect(resolvedPolicy).toBe(
        `{"Version":"2012-10-17","Id":"__default_policy_ID","Statement":[{"Sid":"AllowSQSSubscription","Effect":"Allow","Principal":{"AWS":"*"},"Action":["sns:Subscribe"],"Resource":"arn:aws:sns:eu-central-1:632374391739:test-sns-some-service","Condition":{}}]}`,
      )
    })
  })

  describe('generateFilterAttributes', () => {
    it('resolves filter for a single schema', () => {
      const resolvedFilter = generateFilterAttributes(
        [PERMISSIONS_ADD_MESSAGE_SCHEMA],
        'messageType',
      )

      expect(resolvedFilter).toEqual({
        FilterPolicy: `{"type":["add"]}`,
        FilterPolicyScope: 'MessageBody',
      })
    })

    it('resolves filter for multiple schemas', () => {
      const resolvedFilter = generateFilterAttributes(
        [PERMISSIONS_REMOVE_MESSAGE_SCHEMA, PERMISSIONS_ADD_MESSAGE_SCHEMA],
        'messageType',
      )

      expect(resolvedFilter).toEqual({
        FilterPolicy: `{"type":["remove","add"]}`,
        FilterPolicyScope: 'MessageBody',
      })
    })
  })
})
