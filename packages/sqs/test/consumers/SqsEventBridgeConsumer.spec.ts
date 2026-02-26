import { SendMessageCommand, type SQSClient } from '@aws-sdk/client-sqs'
import { MessageHandlerConfigBuilder } from '@message-queue-toolkit/core'
import type { AwilixContainer } from 'awilix'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'

import { AbstractSqsConsumer } from '../../lib/sqs/AbstractSqsConsumer.ts'
import type { TestAwsResourceAdmin } from '../utils/testAdmin.ts'
import type { Dependencies } from '../utils/testContext.ts'
import { registerDependencies } from '../utils/testContext.ts'
import {
  USER_PRESENCE_ENVELOPE_SCHEMA,
  type UserPresenceEnvelope,
  type UserRoutingStatusEnvelope,
} from './eventBridgeSchemas.ts'
import type { EventBridgeTestContext } from './SqsEventBridgeConsumer.ts'
import { SqsEventBridgeConsumer } from './SqsEventBridgeConsumer.ts'

describe('SqsEventBridgeConsumer', () => {
  let diContainer: AwilixContainer<Dependencies>
  let sqsClient: SQSClient
  let testAdmin: TestAwsResourceAdmin
  let consumer: SqsEventBridgeConsumer
  let executionContext: EventBridgeTestContext

  beforeEach(async () => {
    executionContext = {
      userPresenceMessages: [],
      userRoutingStatusMessages: [],
    }

    diContainer = await registerDependencies()
    sqsClient = diContainer.cradle.sqsClient
    testAdmin = diContainer.cradle.testAdmin

    consumer = new SqsEventBridgeConsumer(diContainer.cradle, executionContext)
    await consumer.start()
  })

  afterEach(async () => {
    await consumer.close()
    await testAdmin.deleteQueues(SqsEventBridgeConsumer.QUEUE_NAME)
    await diContainer.cradle.awilixManager.executeDispose()
    await diContainer.dispose()
  })

  it('should consume EventBridge user presence event', async () => {
    // Arrange: Create an EventBridge-style event
    const eventBridgeEvent = {
      version: '0',
      id: '123e4567-e89b-12d3-a456-426614174000',
      'detail-type': 'v2.users.{id}.presence',
      source: 'genesys.cloud',
      account: '111222333444',
      time: '2025-11-18T12:34:56.789Z',
      region: 'us-east-1',
      resources: [],
      detail: {
        topicName: 'v2.users.{id}.presence',
        userId: 'abcdef12-3456-7890-abcd-ef1234567890',
        organizationId: 'org12345-6789-abcd-ef01-234567890abc',
        presenceDefinition: {
          id: '1',
          systemPresence: 'AVAILABLE',
          mobilePresence: 'OFFLINE',
          aggregationPresence: 'AVAILABLE',
          message: null,
        },
        timestamp: '2025-11-18T12:34:56.789Z',
      },
    } satisfies UserPresenceEnvelope

    // Act: Send the EventBridge event to SQS
    await sqsClient.send(
      new SendMessageCommand({
        QueueUrl: consumer.queueProps.url,
        MessageBody: JSON.stringify(eventBridgeEvent),
      }),
    )

    // Assert: Wait for message to be consumed using handlerSpy
    const spy = await consumer.handlerSpy.waitForMessageWithId(eventBridgeEvent.id, 'consumed')
    expect(spy.processingResult).toEqual({ status: 'consumed' })

    // Verify the handler received the full envelope
    expect(executionContext.userPresenceMessages).toHaveLength(1)
    expect(executionContext.userPresenceMessages[0]).toEqual(eventBridgeEvent)
  })

  it('should consume EventBridge user routing status event', async () => {
    // Arrange: Create an EventBridge routing status event
    const eventBridgeEvent = {
      version: '0',
      id: '223e4567-e89b-12d3-a456-426614174001',
      'detail-type': 'v2.users.{id}.routing.status',
      source: 'genesys.cloud',
      account: '111222333444',
      time: '2025-11-18T13:00:00.000Z',
      region: 'us-east-1',
      resources: [],
      detail: {
        topicName: 'v2.users.{id}.routing.status',
        userId: 'xyz12345-6789-abcd-ef01-234567890def',
        organizationId: 'org12345-6789-abcd-ef01-234567890abc',
        routingStatus: {
          id: 'on-queue',
          status: 'ON_QUEUE',
          startTime: '2025-11-18T13:00:00.000Z',
        },
        timestamp: '2025-11-18T13:00:00.000Z',
      },
    } satisfies UserRoutingStatusEnvelope

    // Act
    await sqsClient.send(
      new SendMessageCommand({
        QueueUrl: consumer.queueProps.url,
        MessageBody: JSON.stringify(eventBridgeEvent),
      }),
    )

    // Assert: Wait for message to be consumed using handlerSpy
    const spy = await consumer.handlerSpy.waitForMessageWithId(eventBridgeEvent.id, 'consumed')
    expect(spy.processingResult).toEqual({ status: 'consumed' })

    expect(executionContext.userRoutingStatusMessages).toHaveLength(1)
    expect(executionContext.userRoutingStatusMessages[0]).toEqual(eventBridgeEvent)
  })

  it('should extract timestamp from envelope even when handler throws error', async () => {
    // This test verifies that custom timestamp field mapping works correctly in error paths.
    // EventBridge uses 'time' instead of 'timestamp', and this test ensures the
    // messageTimestampField configuration is respected when capturing error metadata.

    const capturedMetadata: any[] = []
    const spyMetricsManager = {
      registerProcessedMessage: (metadata: any) => {
        capturedMetadata.push(metadata)
      },
    }

    // Create a handler that throws an error
    const errorThrowingContext: EventBridgeTestContext = {
      userPresenceMessages: [],
      userRoutingStatusMessages: [],
    }

    // We need to create a custom consumer with a failing handler
    class FailingEventBridgeConsumer extends AbstractSqsConsumer<any, any> {
      public static readonly QUEUE_NAME = 'failing_eventbridge_events'

      constructor(dependencies: any, executionContext: any) {
        super(
          dependencies,
          {
            creationConfig: {
              queue: {
                QueueName: FailingEventBridgeConsumer.QUEUE_NAME,
                Attributes: {
                  VisibilityTimeout: '1', // Short visibility timeout to fail fast
                },
              },
            },
            deletionConfig: {
              deleteIfExists: true,
            },
            messageTypeResolver: { messageTypePath: 'detail-type' },
            messageIdField: 'id',
            messageTimestampField: 'time',
            handlerSpy: true,
            maxRetryDuration: 0, // Don't retry - fail immediately
            handlers: new MessageHandlerConfigBuilder<any, any>()
              .addConfig(USER_PRESENCE_ENVELOPE_SCHEMA, (_message, _context) => {
                // Always throw error to trigger error path
                throw new Error('Intentional test error')
              })
              .build(),
          },
          executionContext,
        )
      }

      public get queueProps() {
        return {
          name: this.queueName,
          url: this.queueUrl,
          arn: this.queueArn,
        }
      }
    }

    const failingConsumer = new FailingEventBridgeConsumer(
      { ...diContainer.cradle, messageMetricsManager: spyMetricsManager },
      errorThrowingContext,
    )
    await failingConsumer.start()

    // Create EventBridge event with DIFFERENT timestamps in envelope vs payload
    const envelopeTimestamp = '2025-11-18T16:30:45.123Z'
    const payloadTimestampMs = new Date('2025-11-18T16:30:00.000Z').getTime()
    const eventBridgeEvent = {
      version: '0',
      id: 'error-test-1',
      'detail-type': 'v2.users.{id}.presence',
      source: 'genesys.cloud',
      account: '111222333444',
      time: envelopeTimestamp, // Envelope timestamp
      region: 'us-east-1',
      resources: [],
      detail: {
        topicName: 'v2.users.{id}.presence',
        userId: 'error-test-user',
        organizationId: 'org-error',
        presenceDefinition: {
          id: '1',
          systemPresence: 'AVAILABLE',
          mobilePresence: 'OFFLINE',
          aggregationPresence: 'AVAILABLE',
          message: null,
        },
        timestamp: '2025-11-18T16:30:00.000Z', // Different timestamp in payload
      },
    }

    // Send message
    await sqsClient.send(
      new SendMessageCommand({
        QueueUrl: failingConsumer.queueProps.url,
        MessageBody: JSON.stringify(eventBridgeEvent),
      }),
    )

    // Wait a bit for message to be processed
    await new Promise((resolve) => setTimeout(resolve, 2000))

    // CRITICAL ASSERTION: Verify metrics manager received metadata with ENVELOPE timestamp
    // This verifies that messageTimestampField: 'time' configuration is correctly used
    // to extract timestamps from EventBridge's 'time' field, not 'timestamp' field
    const envelopeTimestampMs = new Date(envelopeTimestamp).getTime()

    // Find error metadata for our queue
    const errorMetadata = capturedMetadata.find(
      (m) => m.queueName === 'failing_eventbridge_events' && m.processingResult?.status === 'error',
    )

    expect(errorMetadata).toBeDefined()
    // This is the key assertion: timestamp should be from envelope, not payload
    expect(errorMetadata.messageTimestamp).toBe(envelopeTimestampMs)
    expect(errorMetadata.messageTimestamp).not.toBe(payloadTimestampMs)

    // Verify the message stored is the full envelope
    expect(errorMetadata.message).toHaveProperty('version') // Envelope field
    expect(errorMetadata.message).toHaveProperty('time') // Envelope field
    expect(errorMetadata.message).toHaveProperty('detail')
    expect(errorMetadata.message.detail).toHaveProperty('userId', 'error-test-user')

    // Cleanup
    await failingConsumer.close()
    await testAdmin.deleteQueues(failingConsumer.queueProps.name)
  }, 10000) // 10 second timeout

  it('should handle multiple EventBridge events', async () => {
    // Arrange
    const presenceEvent = {
      version: '0',
      id: 'presence-1',
      'detail-type': 'v2.users.{id}.presence',
      source: 'genesys.cloud',
      account: '111222333444',
      time: '2025-11-18T14:00:00.000Z',
      region: 'us-east-1',
      resources: [],
      detail: {
        topicName: 'v2.users.{id}.presence',
        userId: 'user1',
        organizationId: 'org1',
        presenceDefinition: {
          id: '1',
          systemPresence: 'AVAILABLE',
          mobilePresence: 'ONLINE',
          aggregationPresence: 'AVAILABLE',
          message: null,
        },
        timestamp: '2025-11-18T14:00:00.000Z',
      },
    } satisfies UserPresenceEnvelope

    const routingEvent = {
      version: '0',
      id: 'routing-1',
      'detail-type': 'v2.users.{id}.routing.status',
      source: 'genesys.cloud',
      account: '111222333444',
      time: '2025-11-18T14:05:00.000Z',
      region: 'us-east-1',
      resources: [],
      detail: {
        topicName: 'v2.users.{id}.routing.status',
        userId: 'user2',
        organizationId: 'org1',
        routingStatus: {
          id: 'idle',
          status: 'IDLE',
        },
        timestamp: '2025-11-18T14:05:00.000Z',
      },
    } satisfies UserRoutingStatusEnvelope

    // Act: Send both events
    await sqsClient.send(
      new SendMessageCommand({
        QueueUrl: consumer.queueProps.url,
        MessageBody: JSON.stringify(presenceEvent),
      }),
    )
    await sqsClient.send(
      new SendMessageCommand({
        QueueUrl: consumer.queueProps.url,
        MessageBody: JSON.stringify(routingEvent),
      }),
    )

    // Assert: Wait for both messages using handlerSpy
    const presenceSpy = await consumer.handlerSpy.waitForMessageWithId(presenceEvent.id, 'consumed')
    expect(presenceSpy.processingResult).toEqual({ status: 'consumed' })

    const routingSpy = await consumer.handlerSpy.waitForMessageWithId(routingEvent.id, 'consumed')
    expect(routingSpy.processingResult).toEqual({ status: 'consumed' })

    expect(executionContext.userPresenceMessages).toHaveLength(1)
    expect(executionContext.userRoutingStatusMessages).toHaveLength(1)
    expect(executionContext.userPresenceMessages[0]?.detail.userId).toBe('user1')
    expect(executionContext.userRoutingStatusMessages[0]?.detail.userId).toBe('user2')
  })
})
