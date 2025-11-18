import { SendMessageCommand, type SQSClient } from '@aws-sdk/client-sqs'
import type { AwilixContainer } from 'awilix'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'

import { deleteQueue } from '../../lib/utils/sqsUtils.ts'
import type { Dependencies } from '../utils/testContext.ts'
import { registerDependencies } from '../utils/testContext.ts'
import type { UserPresenceEnvelope, UserRoutingStatusEnvelope } from './eventBridgeSchemas.ts'
import type { EventBridgeTestContext } from './SqsEventBridgeConsumer.ts'
import { SqsEventBridgeConsumer } from './SqsEventBridgeConsumer.ts'

describe('SqsEventBridgeConsumer', () => {
  let diContainer: AwilixContainer<Dependencies>
  let sqsClient: SQSClient
  let consumer: SqsEventBridgeConsumer
  let executionContext: EventBridgeTestContext

  beforeEach(async () => {
    executionContext = {
      userPresenceMessages: [],
      userRoutingStatusMessages: [],
    }

    diContainer = await registerDependencies()
    sqsClient = diContainer.cradle.sqsClient

    consumer = new SqsEventBridgeConsumer(diContainer.cradle, executionContext)
    await consumer.start()
  })

  afterEach(async () => {
    await consumer.close()
    await deleteQueue(sqsClient, SqsEventBridgeConsumer.QUEUE_NAME)
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

    // Verify the handler received only the 'detail' field content, not the full envelope
    expect(executionContext.userPresenceMessages).toHaveLength(1)
    expect(executionContext.userPresenceMessages[0]).toEqual(eventBridgeEvent.detail)

    // Verify the handler did NOT receive the envelope fields
    expect(executionContext.userPresenceMessages[0]).not.toHaveProperty('version')
    expect(executionContext.userPresenceMessages[0]).not.toHaveProperty('source')
    expect(executionContext.userPresenceMessages[0]).not.toHaveProperty('account')
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
    expect(executionContext.userRoutingStatusMessages[0]).toEqual(eventBridgeEvent.detail)
  })

  it('should extract timestamp from envelope for metadata when messageTimestampFromFullMessage is true', async () => {
    // Arrange: Create an EventBridge event with timestamp ONLY in envelope
    const envelopeTimestamp = '2025-11-18T15:30:45.123Z'
    const eventBridgeEvent = {
      version: '0',
      id: 'timestamp-test-1',
      'detail-type': 'v2.users.{id}.presence',
      source: 'genesys.cloud',
      account: '111222333444',
      time: envelopeTimestamp, // Timestamp in envelope (ISO 8601 string)
      region: 'us-east-1',
      resources: [],
      detail: {
        topicName: 'v2.users.{id}.presence',
        userId: 'timestamp-test-user',
        organizationId: 'org-timestamp',
        presenceDefinition: {
          id: '1',
          systemPresence: 'AVAILABLE',
          mobilePresence: 'OFFLINE',
          aggregationPresence: 'AVAILABLE',
          message: null,
        },
        timestamp: '2025-11-18T15:30:00.000Z', // Different timestamp in detail (not used for metadata)
      },
    } satisfies UserPresenceEnvelope

    // Act
    await sqsClient.send(
      new SendMessageCommand({
        QueueUrl: consumer.queueProps.url,
        MessageBody: JSON.stringify(eventBridgeEvent),
      }),
    )

    // Assert: Verify metadata extraction used envelope timestamp, not payload timestamp
    const processedMessages = await consumer.handlerSpy.waitForMessageWithId(
      eventBridgeEvent.id,
      'consumed',
    )

    // Verify processing succeeded
    expect(processedMessages.processingResult).toEqual({ status: 'consumed' })

    // Verify handler received only the detail (payload), not the envelope
    expect(executionContext.userPresenceMessages).toHaveLength(1)
    expect(executionContext.userPresenceMessages[0]).toEqual(eventBridgeEvent.detail)

    // Key assertion: Verify the originalMessage stored in handlerSpy contains the envelope
    // with the 'time' field (not the detail's 'timestamp' field)
    expect(processedMessages.message).toBeDefined()
    // The message should be the full envelope (originalMessage), which has 'time' field
    expect(processedMessages.message).toHaveProperty('time', envelopeTimestamp)
    expect(processedMessages.message).toHaveProperty('detail-type', 'v2.users.{id}.presence')

    // Verify it's the envelope, not just the detail
    expect(processedMessages.message).toHaveProperty('version', '0')
    expect(processedMessages.message).toHaveProperty('source', 'genesys.cloud')
  })

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
    expect(executionContext.userPresenceMessages[0]?.userId).toBe('user1')
    expect(executionContext.userRoutingStatusMessages[0]?.userId).toBe('user2')
  })
})
