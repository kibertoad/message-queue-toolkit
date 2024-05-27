import type {
  CommonCreationConfigType,
  QueueConsumerDependencies,
  QueueDependencies,
  QueueOptions,
} from '@message-queue-toolkit/core'
import { AbstractQueueService } from '@message-queue-toolkit/core'
import type { Channel, Connection, Message } from 'amqplib'
import type { Options } from 'amqplib/properties'

import type { AmqpConnectionManager, ConnectionReceiver } from './AmqpConnectionManager'

export type AMQPDependencies = QueueDependencies & {
  amqpConnectionManager: AmqpConnectionManager
}

export type AMQPConsumerDependencies = AMQPDependencies & QueueConsumerDependencies
export type AMQPQueueConfig = Options.AssertQueue

export type AMQPQueueCreationConfig = {
  queueOptions: AMQPQueueConfig
  queueName: string
  updateAttributesIfExists?: boolean
}

export type AMQPTopicCreationConfig = AMQPQueueCreationConfig & {
  exchange: string
  topicPattern?: string // defaults to '', which is accepted by RabbitMQ
}

export type AMQPTopicPublisherConfig = {
  exchange: string
} & CommonCreationConfigType

export type AMQPQueueLocator = {
  queueName: string
}

export type AMQPTopicLocator = AMQPQueueLocator & {
  exchange: string
}

export abstract class AbstractAmqpService<
    MessagePayloadType extends object,
    DependenciesType extends AMQPDependencies = AMQPDependencies,
    ExecutionContext = unknown,
    PrehandlerOutput = unknown,
    CreationConfig extends CommonCreationConfigType = AMQPQueueCreationConfig,
    LocatorConfig extends object = AMQPQueueLocator,
  >
  extends AbstractQueueService<
    MessagePayloadType,
    Message,
    DependenciesType,
    CreationConfig,
    LocatorConfig,
    QueueOptions<CreationConfig, LocatorConfig>,
    ExecutionContext,
    PrehandlerOutput
  >
  implements ConnectionReceiver
{
  protected connection?: Connection
  private connectionManager: AmqpConnectionManager
  // @ts-ignore
  protected channel: Channel
  private isShuttingDown: boolean

  constructor(
    dependencies: DependenciesType,
    options: QueueOptions<CreationConfig, LocatorConfig>,
  ) {
    super(dependencies, options)

    this.isShuttingDown = false
    this.connectionManager = dependencies.amqpConnectionManager
    this.connection = this.connectionManager.getConnectionSync()
    this.connectionManager.subscribeConnectionReceiver(this)
  }

  async receiveNewConnection(connection: Connection) {
    this.connection = connection

    this.isShuttingDown = false
    // If channel already exists, recreate it
    const oldChannel = this.channel

    try {
      this.channel = await this.connection.createChannel()
    } catch (err) {
      // @ts-ignore
      this.logger.error(`Error creating channel: ${err.message}`)
      await this.connectionManager.reconnect()
      return
    }

    if (oldChannel) {
      this.isShuttingDown = true
      try {
        await oldChannel.close()
      } catch {
        // errors are ok
      }
      this.isShuttingDown = false
    }

    this.channel.on('close', () => {
      if (!this.isShuttingDown) {
        this.logger.error(`AMQP connection lost!`)
        this.reconnect().catch((err) => {
          this.handleError(err)
          throw err
        })
      }
    })
    this.channel.on('error', (err) => {
      this.handleError(err)
    })

    await this.createMissingEntities()
  }

  protected abstract createMissingEntities(): Promise<void>

  private async destroyChannel(): Promise<void> {
    if (this.channel) {
      try {
        await this.channel.close()
      } catch (err) {
        // We don't care about connection closing errors
      } finally {
        // @ts-ignore
        this.channel = undefined
      }
    }
  }

  public async init() {
    if (this.creationConfig?.updateAttributesIfExists) {
      throw new Error(
        'updateAttributesIfExists parameter is not currently supported by the AMQP adapter',
      )
    }

    // if we don't have connection yet, it's fine, we'll wait for a later receiveNewConnection() call
    if (this.connection) {
      await this.receiveNewConnection(this.connection)
    }
    this.isInitted = true
  }

  public async reconnect() {
    await this.connectionManager.reconnect()
  }

  async close(): Promise<void> {
    this.isShuttingDown = true
    await this.destroyChannel()
    this.isInitted = false
  }
}
