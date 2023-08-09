import { describe } from 'vitest'

import {
  generateQueuePublishForTopicPolicy,
  generateWildcardSnsArn,
  generateWildcardSqsArn,
} from './sqsAttributeUtils'

describe('sqsAttributeUtils', () => {
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

  describe('generateWildcardSqsArn', () => {
    it('Generates wildcard ARN', () => {
      const arn = generateWildcardSqsArn('test-service*')

      expect(arn).toBe('arn:aws:sqs:*:*:test-service*')
    })
  })

  describe('generateWildcardSnsArn', () => {
    it('Generates wildcard ARN', () => {
      const arn = generateWildcardSnsArn('test-service*')

      expect(arn).toBe('arn:aws:sns:*:*:test-service*')
    })
  })
})
