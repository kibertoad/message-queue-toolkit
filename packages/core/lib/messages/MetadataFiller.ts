import { randomUUID } from 'node:crypto'

import type { BaseEventType } from '../events/baseEventSchemas'

import type { MessageMetadataType } from './baseMessageSchemas'

export type IdGenerator = () => string
export type TimestampGenerator = () => string

export type MetadataFillerOptions = {
  serviceId: string
  schemaVersion: string
  idGenerator?: IdGenerator
  timestampGenerator?: TimestampGenerator
}

export type MetadataFiller<T extends BaseEventType = BaseEventType, M = MessageMetadataType> = {
  produceMetadata(currentMessage: T, precedingMessageMetadata?: M): M
  produceId(): string
  produceTimestamp(): string
}

export class CommonMetadataFiller implements MetadataFiller<BaseEventType, MessageMetadataType> {
  private readonly serviceId: string
  private readonly schemaVersion: string
  public readonly produceId: IdGenerator
  public readonly produceTimestamp: TimestampGenerator

  constructor(options: MetadataFillerOptions) {
    this.serviceId = options.serviceId
    this.schemaVersion = options.schemaVersion
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
    _currentMessage: BaseEventType,
    precedingMessageMetadata?: MessageMetadataType,
  ): MessageMetadataType {
    return {
      producedBy: this.serviceId,
      originatedFrom: precedingMessageMetadata?.originatedFrom ?? this.serviceId,
      schemaVersion: this.schemaVersion,
      correlationId: precedingMessageMetadata?.correlationId ?? this.produceId(),
    }
  }
}
