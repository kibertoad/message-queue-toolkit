import type { SyncPublisher } from '@message-queue-toolkit/core'

import { AbstractAmqpService } from './AbstractAmqpService'
import type { CommonMessage } from './types/MessageTypes'
import { buildQueueMessage } from './utils/queueUtils'

export abstract class AbstractAmqpPublisher<MessagePayloadType extends CommonMessage>
  extends AbstractAmqpService<MessagePayloadType>
  implements SyncPublisher<MessagePayloadType>
{
  publish(message: MessagePayloadType): void {
    this.channel.sendToQueue(this.queueName, buildQueueMessage(message))
  }
}
