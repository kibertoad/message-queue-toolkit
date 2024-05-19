import type { DeletionConfig } from '@message-queue-toolkit/core'
import { isProduction } from '@message-queue-toolkit/core'
import type { Channel, Connection } from 'amqplib'

import type { AMQPCreationConfig, AMQPLocator } from '../AbstractAmqpService'

export async function checkQueueExists(connection: Connection, locatorConfig: AMQPLocator) {
  // queue check breaks channel if not successful
  const checkChannel = await connection.createChannel()
  checkChannel.on('error', () => {
    // it's OK
  })
  try {
    await checkChannel.checkQueue(locatorConfig.queueName)
    await checkChannel.close()
  } catch (err) {
    throw new Error(`Queue with queueName ${locatorConfig.queueName} does not exist.`)
  }
}

export async function ensureAmqpQueue(
  connection: Connection,
  channel: Channel,
  creationConfig?: AMQPCreationConfig,
  locatorConfig?: AMQPLocator,
) {
  if (creationConfig) {
    await channel.assertQueue(creationConfig.queueName, creationConfig.queueOptions)
  } else {
    if (!locatorConfig) {
      throw new Error('locatorConfig is mandatory when creationConfig is not set')
    }
    await checkQueueExists(connection, locatorConfig)
  }
}

export async function deleteAmqpQueue(
  channel: Channel,
  deletionConfig: DeletionConfig,
  creationConfig: AMQPCreationConfig,
) {
  if (!deletionConfig.deleteIfExists) {
    return
  }

  if (isProduction() && !deletionConfig.forceDeleteInProduction) {
    throw new Error(
      'You are running autodeletion in production. This can and probably will cause a loss of data. If you are absolutely sure you want to do this, please set deletionConfig.forceDeleteInProduction to true',
    )
  }

  if (!creationConfig.queueName) {
    throw new Error('QueueName must be set for automatic deletion')
  }

  await channel.deleteQueue(creationConfig.queueName)
}
