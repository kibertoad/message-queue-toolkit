import type { SyncPublisher } from '@message-queue-toolkit/core'

import { AbstractAmqpService } from './AbstractAmqpService'
import { objectToBuffer } from '../../core/lib/utils/queueUtils'

export abstract class AbstractAmqpPublisher<MessagePayloadType extends {}>
  extends AbstractAmqpService<MessagePayloadType>
  implements SyncPublisher<MessagePayloadType>
{
  publish(message: MessagePayloadType): void {
    this.channel.sendToQueue(this.queueName, objectToBuffer(message))
  }
}
