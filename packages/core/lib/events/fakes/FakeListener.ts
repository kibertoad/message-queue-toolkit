import type { PublisherMessageMetadataType } from '@message-queue-toolkit/schemas'
import type { AnyEventHandler, CommonEventDefinition } from '../eventTypes'

export class FakeListener<SupportedEvents extends CommonEventDefinition[]>
  implements AnyEventHandler<SupportedEvents>
{
  public receivedEvents: SupportedEvents[number]['publisherSchema']['_output'][] = []
  public receivedMetadata: PublisherMessageMetadataType[] = []

  constructor(_supportedEvents: SupportedEvents) {
    this.receivedEvents = []
  }

  handleEvent(
    event: SupportedEvents[number]['publisherSchema']['_output'],
    metadata: PublisherMessageMetadataType,
  ): void | Promise<void> {
    this.receivedEvents.push(event)
    this.receivedMetadata.push(metadata)
  }
}
