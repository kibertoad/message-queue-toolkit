import type { SQSClient } from '@aws-sdk/client-sqs'
import { ReceiveMessageCommand } from '@aws-sdk/client-sqs'
import { waitAndRetry } from '@message-queue-toolkit/core'
import type { AwilixContainer } from 'awilix'
import { asClass } from 'awilix'
import { describe, beforeEach, afterEach, expect, it } from 'vitest'
import z from 'zod'

import { FakeConsumerErrorResolver } from '../fakes/FakeConsumerErrorResolver'
import type { SqsPermissionPublisherMonoSchema } from '../publishers/SqsPermissionPublisherMonoSchema'
import { userPermissionMap } from '../repositories/PermissionRepository'
import { registerDependencies, SINGLETON_CONFIG } from '../utils/testContext'
import type { Dependencies } from '../utils/testContext'

const perms: [string, ...string[]] = ['perm1', 'perm2']

describe('SqsPermissionsConsumerMonoSchema', () => {
  describe('error handling', () => {
    let diContainer: AwilixContainer<Dependencies>
    let publisher: SqsPermissionPublisherMonoSchema
    let sqsClient: SQSClient

    beforeEach(async () => {
      diContainer = await registerDependencies({
        consumerErrorResolver: asClass(FakeConsumerErrorResolver, SINGLETON_CONFIG),
      })
      sqsClient = diContainer.cradle.sqsClient
      publisher = diContainer.cradle.permissionPublisher

      delete userPermissionMap[100]
      delete userPermissionMap[200]
      delete userPermissionMap[300]

      const command = new ReceiveMessageCommand({
        QueueUrl: diContainer.cradle.permissionPublisher.queueUrl,
      })
      const reply = await sqsClient.send(command)
      expect(reply.Messages!.length).toBe(0)

      const fakeErrorResolver = diContainer.cradle
        .consumerErrorResolver as FakeConsumerErrorResolver
      fakeErrorResolver.clear()
    })

    afterEach(async () => {
      await diContainer.cradle.awilixManager.executeDispose()
      await diContainer.dispose()
    })

    it('Invalid message in the queue', async () => {
      const { consumerErrorResolver } = diContainer.cradle

      // @ts-ignore
      publisher['messageSchema'] = z.any()
      await publisher.publish({
        messageType: 'add',
        permissions: perms,
      } as any)

      const fakeResolver = consumerErrorResolver as FakeConsumerErrorResolver
      await waitAndRetry(() => {
        return fakeResolver.handleErrorCallsCount > 0
      })

      expect(fakeResolver.handleErrorCallsCount).toBe(1)
      expect(fakeResolver.errors[0].message).toContain('"received": "undefined"')
    })

    it('Non-JSON message in the queue', async () => {
      const { consumerErrorResolver } = diContainer.cradle

      // @ts-ignore
      publisher['messageSchema'] = z.any()
      await publisher.publish('dummy' as any)

      const fakeResolver = consumerErrorResolver as FakeConsumerErrorResolver
      const errorCount = await waitAndRetry(() => {
        return fakeResolver.handleErrorCallsCount
      })

      expect(errorCount).toBe(1)
      expect(fakeResolver.errors[0].message).toContain('Expected object, received string')
    })
  })
})
