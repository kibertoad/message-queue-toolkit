import type { DeletionConfig } from '@message-queue-toolkit/core'
import { isProduction } from '@message-queue-toolkit/core'
import type { Channel, ChannelModel } from 'amqplib'

import type {
  AMQPQueueCreationConfig,
  AMQPQueueLocator,
  AMQPTopicCreationConfig,
  AMQPTopicLocator,
  AMQPTopicPublisherConfig,
} from '../AbstractAmqpService'

export async function checkQueueExists(
  connection: ChannelModel,
  locatorConfig: AMQPQueueLocator,
): Promise<void> {
  // queue check breaks channel if not successful
  const checkChannel = await connection.createChannel()
  checkChannel.on('error', () => {
    // it's OK
  })
  try {
    await checkChannel.checkQueue(locatorConfig.queueName)
    await checkChannel.close()
  } catch (_err) {
    throw new Error(`Queue with queueName ${locatorConfig.queueName} does not exist.`)
  }
}

export async function checkExchangeExists(
  connection: ChannelModel,
  locatorConfig: AMQPTopicPublisherConfig,
): Promise<void> {
  // exchange check breaks channel if not successful
  const checkChannel = await connection.createChannel()
  checkChannel.on('error', () => {
    // it's OK
  })
  try {
    await checkChannel.checkExchange(locatorConfig.exchange)
    await checkChannel.close()
  } catch (_err) {
    throw new Error(`Exchange ${locatorConfig.exchange} does not exist.`)
  }
}

export async function ensureAmqpQueue(
  connection: ChannelModel,
  channel: Channel,
  creationConfig?: AMQPQueueCreationConfig,
  locatorConfig?: AMQPQueueLocator,
): Promise<void> {
  if (creationConfig) {
    await channel.assertQueue(creationConfig.queueName, creationConfig.queueOptions)
  } else {
    if (!locatorConfig) {
      throw new Error('locatorConfig is mandatory when creationConfig is not set')
    }
    await checkQueueExists(connection, locatorConfig)
  }
}

export async function ensureAmqpTopicSubscription(
  connection: ChannelModel,
  channel: Channel,
  creationConfig?: AMQPTopicCreationConfig,
  locatorConfig?: AMQPTopicLocator,
): Promise<void> {
  await ensureAmqpQueue(connection, channel, creationConfig, locatorConfig)

  if (creationConfig) {
    await channel.assertExchange(creationConfig.exchange, 'topic')
    await channel.bindQueue(
      creationConfig.queueName,
      creationConfig.exchange,
      creationConfig.topicPattern ?? '',
    )
  } else {
    if (!locatorConfig) {
      throw new Error('locatorConfig is mandatory when creationConfig is not set')
    }
    await checkExchangeExists(connection, locatorConfig)
  }
}

export async function ensureExchange(
  connection: ChannelModel,
  channel: Channel,
  creationConfig?: AMQPTopicPublisherConfig,
  locatorConfig?: AMQPTopicPublisherConfig,
): Promise<void> {
  if (creationConfig) {
    await channel.assertExchange(creationConfig.exchange, 'topic')
  } else {
    if (!locatorConfig) {
      throw new Error('locatorConfig is mandatory when creationConfig is not set')
    }
    await checkExchangeExists(connection, locatorConfig)
  }
}

export async function deleteAmqpQueue(
  channel: Channel,
  deletionConfig: DeletionConfig,
  creationConfig: AMQPQueueCreationConfig,
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
