import type { DeletionConfig } from '@message-queue-toolkit/core'
import { isProduction } from '@message-queue-toolkit/core'
import type { Channel } from 'amqplib'

import type { CreateAMQPQueueOptions } from '../AbstractAmqpService'

export async function deleteAmqp(
  channel: Channel,
  deletionConfig: DeletionConfig,
  creationConfig: CreateAMQPQueueOptions,
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
