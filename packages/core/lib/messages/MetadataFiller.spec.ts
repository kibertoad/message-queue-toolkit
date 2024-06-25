import { beforeAll, describe, expect, it } from 'vitest'
import { CommonMetadataFiller } from './MetadataFiller'

const TEST_MESSAGE = {
  type: 'test.message',
}

const EVENT_DEFINITION = {
  schemaVersion: '0.0.0',
}

const SERVICE_ID = 'myServiceId'

describe('MetadataFiller', () => {
  describe('produceMetadata', () => {
    let filler: CommonMetadataFiller

    beforeAll(() => {
      filler = new CommonMetadataFiller({ serviceId: SERVICE_ID })
    })

    it('Autofills metadata if not provided', () => {
      // When
      const metadata = filler.produceMetadata(TEST_MESSAGE, EVENT_DEFINITION)

      // Then
      expect(metadata.schemaVersion).toEqual(EVENT_DEFINITION.schemaVersion)
      expect(metadata.producedBy).toEqual(SERVICE_ID)
      expect(metadata.originatedFrom).toEqual(SERVICE_ID)
      expect(metadata.correlationId).toEqual(expect.any(String))
    })

    it('Autofills metadata if not provided and fallsback to default schema version if not in event definition', () => {
      // When
      const metadata = filler.produceMetadata(TEST_MESSAGE, {})

      // Then
      expect(metadata.schemaVersion).toEqual('1.0.0')
      expect(metadata.producedBy).toEqual(SERVICE_ID)
      expect(metadata.originatedFrom).toEqual(SERVICE_ID)
      expect(metadata.correlationId).toEqual(expect.any(String))
    })

    it('Applies provided metadata', () => {
      // Given
      const providedMetadata = {
        schemaVersion: '2.0.0',
        producedBy: 'producer',
        originatedFrom: 'source',
        correlationId: 'myCorrelationId',
      }

      // When
      const metadata = filler.produceMetadata(TEST_MESSAGE, EVENT_DEFINITION, providedMetadata)

      // Then
      expect(metadata).toEqual(providedMetadata)
    })
  })
})
