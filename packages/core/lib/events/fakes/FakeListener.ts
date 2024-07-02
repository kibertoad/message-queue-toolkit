import type { ConsumerMessageMetadataType } from '@message-queue-toolkit/schemas'
import type { AnyEventHandler, CommonEventDefinition } from '../eventTypes'

export class FakeListener<SupportedEvents extends CommonEventDefinition[]>
  implements AnyEventHandler<SupportedEvents>
{
  public receivedEvents: SupportedEvents[number]['consumerSchema']['_output'][] = []
  public receivedMetadata: ConsumerMessageMetadataType[] = []

  constructor(_supportedEvents: SupportedEvents) {
    this.receivedEvents = []
  }

  handleEvent(
    event: SupportedEvents[number]['consumerSchema']['_output'],
    metadata: ConsumerMessageMetadataType,
  ): void | Promise<void> {
    this.receivedEvents.push(event)
    this.receivedMetadata.push(metadata)
  }
}
