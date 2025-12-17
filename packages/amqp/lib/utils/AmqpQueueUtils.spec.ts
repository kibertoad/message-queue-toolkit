import { reloadConfig } from '@message-queue-toolkit/core'
import type { AwilixContainer } from 'awilix'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { TEST_AMQP_CONFIG } from '../../test/utils/testAmqpConfig.ts'
import type { Dependencies } from '../../test/utils/testContext.ts'
import { registerDependencies } from '../../test/utils/testContext.ts'
import {
  checkExchangeExists,
  deleteAmqpQueue,
  ensureAmqpQueue,
  ensureAmqpTopicSubscription,
  ensureExchange,
} from './amqpQueueUtils.ts'

describe('AmqpQueueUtils', () => {
  let diContainer: AwilixContainer<Dependencies>

  beforeEach(async () => {
    diContainer = await registerDependencies(TEST_AMQP_CONFIG, undefined, false)
  })

  afterEach(async () => {
    const { awilixManager } = diContainer.cradle
    await awilixManager.executeDispose()
    await diContainer.dispose()
  })

  describe('ensureExchange', () => {
    it('creates exchange when creationConfig is provided', async () => {
      const connection = await diContainer.cradle.amqpConnectionManager.getConnection()
      const channel = await connection.createChannel()

      await ensureExchange(connection, channel, { exchange: 'test-exchange' })

      // Verify exchange exists
      await expect(channel.checkExchange('test-exchange')).resolves.not.toThrow()

      await channel.deleteExchange('test-exchange')
      await channel.close()
    })

    it('throws when locatorConfig exchange does not exist', async () => {
      const connection = await diContainer.cradle.amqpConnectionManager.getConnection()
      const channel = await connection.createChannel()

      await expect(
        ensureExchange(connection, channel, undefined, { exchange: 'non-existent-exchange' }),
      ).rejects.toThrow('Exchange non-existent-exchange does not exist.')

      await channel.close()
    })

    it('throws when neither creationConfig nor locatorConfig is provided', async () => {
      const connection = await diContainer.cradle.amqpConnectionManager.getConnection()
      const channel = await connection.createChannel()

      await expect(ensureExchange(connection, channel)).rejects.toThrow(
        'locatorConfig is mandatory when creationConfig is not set',
      )

      await channel.close()
    })

    it('validates exchange exists when locatorConfig is provided', async () => {
      const connection = await diContainer.cradle.amqpConnectionManager.getConnection()
      const channel = await connection.createChannel()

      // First create the exchange
      await channel.assertExchange('existing-exchange', 'topic')

      // Then verify it with locatorConfig
      await expect(
        ensureExchange(connection, channel, undefined, { exchange: 'existing-exchange' }),
      ).resolves.not.toThrow()

      await channel.deleteExchange('existing-exchange')
      await channel.close()
    })
  })

  describe('checkExchangeExists', () => {
    it('throws when exchange does not exist', async () => {
      const connection = await diContainer.cradle.amqpConnectionManager.getConnection()

      await expect(checkExchangeExists(connection, { exchange: 'non-existent' })).rejects.toThrow(
        'Exchange non-existent does not exist.',
      )
    })

    it('does not throw when exchange exists', async () => {
      const connection = await diContainer.cradle.amqpConnectionManager.getConnection()
      const channel = await connection.createChannel()

      await channel.assertExchange('temp-exchange', 'topic')

      await expect(
        checkExchangeExists(connection, { exchange: 'temp-exchange' }),
      ).resolves.not.toThrow()

      await channel.deleteExchange('temp-exchange')
      await channel.close()
    })
  })

  describe('ensureAmqpQueue', () => {
    it('throws when neither creationConfig nor locatorConfig is provided', async () => {
      const connection = await diContainer.cradle.amqpConnectionManager.getConnection()
      const channel = await connection.createChannel()

      await expect(ensureAmqpQueue(connection, channel)).rejects.toThrow(
        'locatorConfig is mandatory when creationConfig is not set',
      )

      await channel.close()
    })
  })

  describe('ensureAmqpTopicSubscription', () => {
    it('throws when neither creationConfig nor locatorConfig is provided', async () => {
      const connection = await diContainer.cradle.amqpConnectionManager.getConnection()
      const channel = await connection.createChannel()

      await expect(ensureAmqpTopicSubscription(connection, channel)).rejects.toThrow(
        'locatorConfig is mandatory when creationConfig is not set',
      )

      await channel.close()
    })
  })

  describe('deleteAmqpQueue', () => {
    it('does nothing when deleteIfExists is false', async () => {
      const connection = await diContainer.cradle.amqpConnectionManager.getConnection()
      const channel = await connection.createChannel()

      await channel.assertQueue('queue-to-keep')

      await deleteAmqpQueue(
        channel,
        { deleteIfExists: false },
        { queueName: 'queue-to-keep', queueOptions: {} },
      )

      // Queue should still exist
      await expect(channel.checkQueue('queue-to-keep')).resolves.not.toThrow()

      await channel.deleteQueue('queue-to-keep')
      await channel.close()
    })

    it('throws in production without forceDeleteInProduction', async () => {
      const connection = await diContainer.cradle.amqpConnectionManager.getConnection()
      const channel = await connection.createChannel()

      // Mock production environment
      vi.stubEnv('NODE_ENV', 'production')
      reloadConfig()

      try {
        await expect(
          deleteAmqpQueue(
            channel,
            { deleteIfExists: true },
            { queueName: 'test-queue', queueOptions: {} },
          ),
        ).rejects.toThrow('You are running autodeletion in production')
      } finally {
        vi.unstubAllEnvs()
        reloadConfig()
        await channel.close()
      }
    })

    it('throws when queueName is not set', async () => {
      const connection = await diContainer.cradle.amqpConnectionManager.getConnection()
      const channel = await connection.createChannel()

      await expect(
        deleteAmqpQueue(
          channel,
          { deleteIfExists: true, forceDeleteInProduction: true },
          { queueName: '', queueOptions: {} },
        ),
      ).rejects.toThrow('QueueName must be set for automatic deletion')

      await channel.close()
    })

    it('deletes queue when all conditions are met', async () => {
      const connection = await diContainer.cradle.amqpConnectionManager.getConnection()
      const channel = await connection.createChannel()

      await channel.assertQueue('queue-to-delete')

      await deleteAmqpQueue(
        channel,
        { deleteIfExists: true, forceDeleteInProduction: true },
        { queueName: 'queue-to-delete', queueOptions: {} },
      )

      // Queue should no longer exist
      const checkChannel = await connection.createChannel()
      checkChannel.on('error', () => {})
      await expect(checkChannel.checkQueue('queue-to-delete')).rejects.toThrow()

      await channel.close()
    })
  })
})
