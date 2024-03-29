import type {
  QueueConsumerDependencies,
  QueueDependencies,
  QueueOptions,
} from '@message-queue-toolkit/core'
import { AbstractQueueService } from '@message-queue-toolkit/core'
import type { Channel, Connection, Message } from 'amqplib'
import type { Options } from 'amqplib/properties'

import type { AmqpConnectionManager, ConnectionReceiver } from './AmqpConnectionManager'
import { deleteAmqp } from './utils/amqpInitter'

export type AMQPDependencies = QueueDependencies & {
  amqpConnectionManager: AmqpConnectionManager
}

export type AMQPConsumerDependencies = AMQPDependencies & QueueConsumerDependencies
export type AMQPQueueConfig = Options.AssertQueue

export type AMQPCreationConfig = {
  queueOptions: AMQPQueueConfig
  queueName: string
  updateAttributesIfExists?: boolean
}

export type AMQPLocator = {
  queueName: string
}

export abstract class AbstractAmqpService<
    MessagePayloadType extends object,
    DependenciesType extends AMQPDependencies = AMQPDependencies,
    ExecutionContext = unknown,
    PrehandlerOutput = unknown,
  >
  extends AbstractQueueService<
    MessagePayloadType,
    Message,
    DependenciesType,
    AMQPCreationConfig,
    AMQPLocator,
    QueueOptions<AMQPCreationConfig, AMQPLocator>,
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
  protected readonly queueName: string

  constructor(
    dependencies: DependenciesType,
    options: QueueOptions<AMQPCreationConfig, AMQPLocator>,
  ) {
    super(dependencies, options)

    this.queueName = options.locatorConfig
      ? options.locatorConfig.queueName
      : options.creationConfig?.queueName
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

    if (this.deletionConfig && this.creationConfig) {
      await deleteAmqp(this.channel, this.deletionConfig, this.creationConfig)
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

    if (this.creationConfig) {
      await this.channel.assertQueue(
        this.creationConfig.queueName,
        this.creationConfig.queueOptions,
      )
    } else {
      await this.checkQueueExists()
    }
  }

  private async checkQueueExists() {
    // queue check breaks channel if not successful
    const checkChannel = await this.connection!.createChannel()
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
        'updateAttributesIfExists parameter is not currently supported by the Amqp adapter',
      )
    }

    // if we don't have connection yet, it's fine, we'll wait for a later receiveNewConnection() call
    if (this.connection) {
      await this.receiveNewConnection(this.connection)
    }
  }

  public async reconnect() {
    await this.connectionManager.reconnect()
  }

  async close(): Promise<void> {
    this.isShuttingDown = true
    await this.destroyChannel()
  }
}
