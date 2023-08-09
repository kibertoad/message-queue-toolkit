import { describe } from 'vitest'

import {
  PERMISSIONS_ADD_MESSAGE_SCHEMA,
  PERMISSIONS_REMOVE_MESSAGE_SCHEMA,
} from '../../test/consumers/userConsumerSchemas'

import {
  generateFilterAttributes,
  generateQueuePublishForTopicPolicy,
  generateTopicSubscriptionPolicy,
} from './snsAttributeUtils'

describe('snsAttributeUtils', () => {
  describe('generateTopicSubscriptionPolicy', () => {
    it('resolves policy', () => {
      const resolvedPolicy = generateTopicSubscriptionPolicy(
        'arn:aws:sns:eu-central-1:632374391739:test-sns-fss-messages',
        'arn:aws:sqs:eu-central-1:632374391739:test-sqs-*',
      )

      expect(resolvedPolicy).toBe(
        `{"Version":"2012-10-17","Id":"__default_policy_ID","Statement":[{"Sid":"AllowSNSPublish","Effect":"Allow","Principal":{"AWS":"*"},"Action":"sqs:SendMessage","Resource":"arn:aws:sqs:eu-central-1:632374391739:test-sqs-some-service","Condition":{"ArnLike":{"aws:SourceArn":"arn:aws:sns:eu-central-1:632374391739:test-sns-*"}}}]}`,
      )
    })
  })

  describe('generateQueuePublishForTopicPolicy', () => {
    it('resolves policy', () => {
      const resolvedPolicy = generateQueuePublishForTopicPolicy(
        'arn:aws:sqs:eu-central-1:632374391739:test-sqs-some-service',
        'arn:aws:sns:eu-central-1:632374391739:test-sns-*',
      )

      expect(resolvedPolicy).toBe(
        `{"Version":"2012-10-17","Id":"__default_policy_ID","Statement":[{"Sid":"AllowSNSPublish","Effect":"Allow","Principal":{"AWS":"*"},"Action":"sqs:SendMessage","Resource":"arn:aws:sqs:eu-central-1:632374391739:test-sqs-some-service","Condition":{"ArnLike":{"aws:SourceArn":"arn:aws:sns:eu-central-1:632374391739:test-sns-*"}}}]}`,
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
