import type {
  QueueConsumerDependencies,
  QueueDependencies,
  NewQueueOptions,
  ExistingQueueOptions,
} from '@message-queue-toolkit/core'
import { AbstractQueueService } from '@message-queue-toolkit/core'
import type { Channel, Connection, Message } from 'amqplib'
import type { Options } from 'amqplib/properties'

import type { AMQPLocatorType } from './AbstractAmqpBaseConsumer'
import { deleteAmqp } from './utils/amqpInitter'

export type AMQPDependencies = QueueDependencies & {
  amqpConnection: Connection
}

export type AMQPConsumerDependencies = AMQPDependencies & QueueConsumerDependencies
export type AMQPQueueConfig = Options.AssertQueue

export type CreateAMQPQueueOptions = {
  queueOptions: AMQPQueueConfig
  queueName: string
}

export type AMQPQueueLocatorType = {
  queueName: string
}

export abstract class AbstractAmqpService<
  MessagePayloadType extends object,
  DependenciesType extends AMQPDependencies = AMQPDependencies,
> extends AbstractQueueService<
  MessagePayloadType,
  Message,
  DependenciesType,
  CreateAMQPQueueOptions,
  AMQPQueueLocatorType,
  NewQueueOptions<CreateAMQPQueueOptions> | ExistingQueueOptions<AMQPLocatorType>
> {
  protected readonly connection: Connection
  // @ts-ignore
  protected channel: Channel
  private isShuttingDown: boolean
  protected readonly queueName: string

  constructor(
    dependencies: DependenciesType,
    options: NewQueueOptions<CreateAMQPQueueOptions> | ExistingQueueOptions<AMQPLocatorType>,
  ) {
    super(dependencies, options)

    this.queueName = options.locatorConfig
      ? options.locatorConfig.queueName
      : options.creationConfig?.queueName
    this.connection = dependencies.amqpConnection
    this.isShuttingDown = false
  }

  private async destroyConnection(): Promise<void> {
    if (this.channel) {
      try {
        await this.channel.close()
      } finally {
        // @ts-ignore
        this.channel = undefined
      }
    }
  }

  public async init() {
    this.isShuttingDown = false

    // If channel exists, recreate it
    if (this.channel) {
      this.isShuttingDown = true
      await this.destroyConnection()
      this.isShuttingDown = false
    }

    this.channel = await this.connection.createChannel()

    if (this.deletionConfig && this.creationConfig) {
      await deleteAmqp(this.channel, this.deletionConfig, this.creationConfig)
    }

    this.channel.on('close', () => {
      if (!this.isShuttingDown) {
        this.logger.error(`AMQP connection lost!`)
        this.init().catch((err) => {
          this.handleError(err)
          throw err
        })
      }
    })
    this.channel.on('error', (err) => {
      this.handleError(err)
    })

    if (this.creationConfig) {
      await this.channel.assertQueue(
        this.creationConfig.queueName,
        this.creationConfig.queueOptions,
      )
    } else {
      // queue check breaks channel if not successful
      const checkChannel = await this.connection.createChannel()
      checkChannel.on('error', () => {
        // it's OK
      })
      try {
        await checkChannel.checkQueue(this.locatorConfig!.queueName)
        await checkChannel.close()
      } catch (err) {
        throw new Error(`Queue with queueName ${this.locatorConfig!.queueName} does not exist.`)
      }
    }
  }

  async close(): Promise<void> {
    this.isShuttingDown = true
    await this.destroyConnection()
  }
}
