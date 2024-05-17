import { randomUUID } from 'node:crypto'

import type { BaseEventType } from '../events/baseEventSchemas'
import type { CommonEventDefinition } from '../events/eventTypes'

import type { MessageMetadataType } from './baseMessageSchemas'

export type IdGenerator = () => string
export type TimestampGenerator = () => Date

export type MetadataFillerOptions = {
  serviceId: string
  schemaVersion: string
  idGenerator?: IdGenerator
  timestampGenerator?: TimestampGenerator
  defaultVersion?: string
}

export type MetadataFiller<
  T extends BaseEventType = BaseEventType,
  D = CommonEventDefinition,
  M = MessageMetadataType,
> = {
  produceMetadata(currentMessage: T, eventDefinition: D, precedingMessageMetadata?: M): M
  produceId(): string
  produceTimestamp(): Date
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
        return new Date()
      })
  }

  produceMetadata(
    _currentMessage: BaseEventType,
    eventDefinition: CommonEventDefinition,
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
