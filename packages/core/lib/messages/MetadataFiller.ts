import { randomUUID } from 'node:crypto'

import type { PublisherBaseEventType } from '../events/baseEventSchemas'
import type { CommonEventDefinition } from '../events/eventTypes'

import type { MessageMetadataType } from './baseMessageSchemas'

export type IdGenerator = () => string
export type TimestampGenerator = () => string

export type MetadataFillerOptions = {
  serviceId: string
  idGenerator?: IdGenerator
  timestampGenerator?: TimestampGenerator
  defaultVersion?: string
}

export type MetadataFiller<
  T extends PublisherBaseEventType = PublisherBaseEventType,
  D = CommonEventDefinition,
  M = MessageMetadataType,
> = {
  produceMetadata(currentMessage: T, eventDefinition: D, precedingMessageMetadata?: M): M
  produceId(): string
  produceTimestamp(): string
}

export class CommonMetadataFiller implements MetadataFiller {
  private readonly serviceId: string
  public readonly produceId: IdGenerator
  public readonly produceTimestamp: TimestampGenerator
  private readonly defaultVersion: string

  constructor(options: MetadataFillerOptions) {
    this.serviceId = options.serviceId
    this.defaultVersion = options.defaultVersion ?? '1.0.0'
    this.produceId =
      options.idGenerator ??
      (() => {
        return randomUUID()
      })
    this.produceTimestamp =
      options.timestampGenerator ??
      (() => {
        return new Date().toISOString()
      })
  }

  produceMetadata(
    _currentMessage: PublisherBaseEventType,
    eventDefinition: Pick<CommonEventDefinition, 'schemaVersion'>,
    precedingMessageMetadata?: MessageMetadataType,
  ): MessageMetadataType {
    return {
      producedBy: this.serviceId,
      originatedFrom: precedingMessageMetadata?.originatedFrom ?? this.serviceId,
      schemaVersion: eventDefinition.schemaVersion ?? this.defaultVersion,
      correlationId: precedingMessageMetadata?.correlationId ?? this.produceId(),
    }
  }
}
