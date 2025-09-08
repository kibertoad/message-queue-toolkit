import { describe, expect, it } from 'vitest'
import {
  SQS_RESOURCE_ANY,
  SQS_RESOURCE_CURRENT_QUEUE,
  type SQSPolicyConfig,
} from '../sqs/AbstractSqsService.ts'
import {
  generateQueuePolicyFromPolicyConfig,
  generateQueuePublishForTopicPolicy,
  generateWildcardSnsArn,
  generateWildcardSqsArn,
} from './sqsAttributeUtils.ts'

describe('sqsAttributeUtils', () => {
  describe('generateQueuePublishForTopicPolicy', () => {
    it('resolves policy', () => {
      const resolvedPolicy = generateQueuePublishForTopicPolicy(
        'arn:aws:sqs:eu-central-1:632374391739:test-sqs-some-service',
        'arn:aws:sns:eu-central-1:632374391739:test-sns-*',
      )

      expect(resolvedPolicy).toMatchInlineSnapshot(
        `"{"Version":"2012-10-17","Id":"__default_policy_ID","Statement":[{"Sid":"AllowSNSPublish","Effect":"Allow","Principal":{"AWS":"*"},"Action":"sqs:SendMessage","Resource":"arn:aws:sqs:eu-central-1:632374391739:test-sqs-some-service","Condition":{"ArnLike":{"aws:SourceArn":"arn:aws:sns:eu-central-1:632374391739:test-sns-*"}}}]}"`,
      )
    })
  })

  describe('generateQueuePolicyFromPolicyConfig', () => {
    const testQueueArn = 'arn:aws:sqs:eu-central-1:632374391739:test-queue'

    it('generates policy with default values', () => {
      const policyConfig = {
        resource: SQS_RESOURCE_CURRENT_QUEUE,
      } satisfies SQSPolicyConfig

      const result = generateQueuePolicyFromPolicyConfig(testQueueArn, policyConfig)

      expect(result).toMatchInlineSnapshot(
        `"{"Version":"2012-10-17","Resource":"arn:aws:sqs:eu-central-1:632374391739:test-queue","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":["sqs:SendMessage","sqs:GetQueueAttributes","sqs:GetQueueUrl"]}]}"`,
      )
    })

    it('generates policy with custom statement values', () => {
      const policyConfig = {
        resource: SQS_RESOURCE_CURRENT_QUEUE,
        statements: {
          Effect: 'Deny',
          Principal: 'arn:aws:iam::123456789012:user/test-user',
          Action: ['sqs:SendMessage'],
        },
      } satisfies SQSPolicyConfig

      const result = generateQueuePolicyFromPolicyConfig(testQueueArn, policyConfig)

      expect(result).toMatchInlineSnapshot(
        `"{"Version":"2012-10-17","Resource":"arn:aws:sqs:eu-central-1:632374391739:test-queue","Statement":[{"Effect":"Deny","Principal":{"AWS":"arn:aws:iam::123456789012:user/test-user"},"Action":["sqs:SendMessage"]}]}"`,
      )
    })

    it('generates policy with array of statements', () => {
      const policyConfig = {
        resource: SQS_RESOURCE_CURRENT_QUEUE,
        statements: [
          {
            Effect: 'Allow',
            Principal: 'arn:aws:iam::123456789012:user/user1',
            Action: ['sqs:SendMessage'],
          },
          {
            Effect: 'Deny',
            Principal: 'arn:aws:iam::123456789012:user/user2',
            Action: ['sqs:ReceiveMessage'],
          },
        ],
      } satisfies SQSPolicyConfig

      const result = generateQueuePolicyFromPolicyConfig(testQueueArn, policyConfig)

      expect(result).toMatchInlineSnapshot(
        `"{"Version":"2012-10-17","Resource":"arn:aws:sqs:eu-central-1:632374391739:test-queue","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::123456789012:user/user1"},"Action":["sqs:SendMessage"]},{"Effect":"Deny","Principal":{"AWS":"arn:aws:iam::123456789012:user/user2"},"Action":["sqs:ReceiveMessage"]}]}"`,
      )
    })

    it('uses current queue resource', () => {
      const policyConfig = {
        resource: SQS_RESOURCE_CURRENT_QUEUE,
      } satisfies SQSPolicyConfig

      const result = generateQueuePolicyFromPolicyConfig(testQueueArn, policyConfig)

      expect(result).toMatchInlineSnapshot(
        `"{"Version":"2012-10-17","Resource":"arn:aws:sqs:eu-central-1:632374391739:test-queue","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":["sqs:SendMessage","sqs:GetQueueAttributes","sqs:GetQueueUrl"]}]}"`,
      )
    })

    it('uses any resource wildcard', () => {
      const policyConfig = {
        resource: SQS_RESOURCE_ANY,
      } satisfies SQSPolicyConfig

      const result = generateQueuePolicyFromPolicyConfig(testQueueArn, policyConfig)

      expect(result).toMatchInlineSnapshot(
        `"{"Version":"2012-10-17","Resource":"arn:aws:sqs:*:*:*","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":["sqs:SendMessage","sqs:GetQueueAttributes","sqs:GetQueueUrl"]}]}"`,
      )
    })

    it('uses custom resource', () => {
      const customResource = 'arn:aws:sqs:us-east-1:123456789012:custom-queue'
      const policyConfig = {
        resource: customResource,
      } satisfies SQSPolicyConfig

      const result = generateQueuePolicyFromPolicyConfig(testQueueArn, policyConfig)

      expect(result).toMatchInlineSnapshot(
        `"{"Version":"2012-10-17","Resource":"arn:aws:sqs:us-east-1:123456789012:custom-queue","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":["sqs:SendMessage","sqs:GetQueueAttributes","sqs:GetQueueUrl"]}]}"`,
      )
    })

    it('handles mixed statement configurations in array', () => {
      const policyConfig = {
        resource: SQS_RESOURCE_CURRENT_QUEUE,
        statements: [
          {
            Effect: 'Allow',
            Principal: 'arn:aws:iam::123456789012:user/user1',
            Action: ['sqs:SendMessage'],
          },
          {
            // Missing Effect and Principal - should use defaults
            Action: ['sqs:ReceiveMessage'],
          },
          {
            Effect: 'Deny',
            // Missing Principal - should use default
            Action: ['sqs:DeleteMessage'],
          },
        ],
      } satisfies SQSPolicyConfig

      const result = generateQueuePolicyFromPolicyConfig(testQueueArn, policyConfig)

      expect(result).toMatchInlineSnapshot(
        `"{"Version":"2012-10-17","Resource":"arn:aws:sqs:eu-central-1:632374391739:test-queue","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::123456789012:user/user1"},"Action":["sqs:SendMessage"]},{"Effect":"Allow","Principal":{"AWS":"*"},"Action":["sqs:ReceiveMessage"]},{"Effect":"Deny","Principal":{"AWS":"*"},"Action":["sqs:DeleteMessage"]}]}"`,
      )
    })

    it('handles undefined statements', () => {
      const policyConfig = {
        resource: SQS_RESOURCE_CURRENT_QUEUE,
        statements: undefined,
      } satisfies SQSPolicyConfig

      const result = generateQueuePolicyFromPolicyConfig(testQueueArn, policyConfig)

      expect(result).toMatchInlineSnapshot(
        `"{"Version":"2012-10-17","Resource":"arn:aws:sqs:eu-central-1:632374391739:test-queue","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":["sqs:SendMessage","sqs:GetQueueAttributes","sqs:GetQueueUrl"]}]}"`,
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
