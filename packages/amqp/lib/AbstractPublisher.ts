import type { SyncPublisher } from '@message-queue-toolkit/core'

import { AbstractQueueService } from './AbstractQueueService'
import type { CommonMessage } from './types/MessageTypes'
import { buildQueueMessage } from './utils/queueUtils'

export abstract class AbstractPublisher<MessagePayloadType extends CommonMessage>
  extends AbstractQueueService<MessagePayloadType>
  implements SyncPublisher<MessagePayloadType>
{
  publish(message: MessagePayloadType): void {
    this.channel.sendToQueue(this.queueName, buildQueueMessage(message))
  }
}
