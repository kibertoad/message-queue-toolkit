import type { QueueConsumerDependencies } from '@message-queue-toolkit/core'
import { AbstractQueueService } from '@message-queue-toolkit/core'
import type { Channel, Connection } from 'amqplib'

import type { QueueDependencies, QueueOptions } from '../../core/lib/queues/AbstractQueueService'

export type AMQPDependencies = QueueDependencies & {
  amqpConnection: Connection
}

export type AMQPConsumerDependencies = AMQPDependencies & QueueConsumerDependencies

export class AbstractAmqpService<
  MessagePayloadType extends object,
  DependenciesType extends AMQPDependencies = AMQPDependencies,
> extends AbstractQueueService<MessagePayloadType, DependenciesType> {
  protected readonly connection: Connection
  // @ts-ignore
  protected channel: Channel
  private isShuttingDown: boolean

  constructor(dependencies: DependenciesType, options: QueueOptions<MessagePayloadType>) {
    super(dependencies, options)

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

    await this.channel.assertQueue(this.queueName, {
      exclusive: false,
      durable: true,
      autoDelete: false,
    })
  }

  async close(): Promise<void> {
    this.isShuttingDown = true
    await this.destroyConnection()
  }
}
