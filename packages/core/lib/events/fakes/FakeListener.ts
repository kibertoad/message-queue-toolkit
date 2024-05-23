import type { MessageMetadataType } from '../../messages/baseMessageSchemas'
import type { AnyEventHandler, CommonEventDefinition } from '../eventTypes'

export class FakeListener<SupportedEvents extends CommonEventDefinition[]>
  implements AnyEventHandler<SupportedEvents>
{
  public receivedEvents: SupportedEvents[number]['consumerSchema']['_output'][] = []
  public receivedMetadata: MessageMetadataType[] = []

  constructor(_supportedEvents: SupportedEvents) {
    this.receivedEvents = []
  }

  handleEvent(
    event: SupportedEvents[number]['consumerSchema']['_output'],
    metadata: MessageMetadataType,
  ): void | Promise<void> {
    this.receivedEvents.push(event)
    this.receivedMetadata.push(metadata)
  }
}
