import { describe, expect, it } from 'vitest'

import {
  hasCodecEnvelopeShape,
  isCodecEnvelope,
  MessageCodecEnum,
} from '../../lib/codec/messageCodec.ts'

const VALID_BASE64 = Buffer.from('hello compressed world').toString('base64')

describe('isCodecEnvelope — custom knownCodecs', () => {
  const CUSTOM_CODECS = new Set(['lz4', 'brotli'])

  it('returns true when the envelope codec is in the supplied knownCodecs set', () => {
    expect(isCodecEnvelope({ __mqtCodec: 'lz4', __mqtData: VALID_BASE64 }, CUSTOM_CODECS)).toBe(
      true,
    )
  })

  it('returns false for a built-in codec when it is not in the supplied knownCodecs set', () => {
    // zstd is valid with default knownCodecs but must be rejected when not in the custom set
    expect(
      isCodecEnvelope(
        { __mqtCodec: MessageCodecEnum.ZSTD, __mqtData: VALID_BASE64 },
        CUSTOM_CODECS,
      ),
    ).toBe(false)
  })

  it('returns false for a codec that is in neither the default nor the supplied set', () => {
    expect(isCodecEnvelope({ __mqtCodec: 'gzip', __mqtData: VALID_BASE64 }, CUSTOM_CODECS)).toBe(
      false,
    )
  })
})

describe('isCodecEnvelope', () => {
  describe('valid envelopes', () => {
    it('returns true for a well-formed envelope', () => {
      expect(isCodecEnvelope({ __mqtCodec: MessageCodecEnum.ZSTD, __mqtData: VALID_BASE64 })).toBe(
        true,
      )
    })

    it('returns true for an envelope with empty base64 data (zero-byte payload)', () => {
      expect(isCodecEnvelope({ __mqtCodec: MessageCodecEnum.ZSTD, __mqtData: '' })).toBe(true)
    })
  })

  describe('preserved sibling fields — detection is presence-based', () => {
    it('returns true when the envelope carries preserved sibling fields (id, type, …)', () => {
      // Publishers copy identity/routing fields alongside the codec fields so broker-side
      // filtering (e.g. SNS body-scoped FilterPolicy) keeps working on compressed
      // messages. Detection is presence-based and must accept these extra siblings,
      // mirroring how offloaded-payload pointers are detected by marker-field presence.
      expect(
        isCodecEnvelope({
          __mqtCodec: MessageCodecEnum.ZSTD,
          __mqtData: VALID_BASE64,
          id: 'real-message',
          type: 'permissions.add',
          timestamp: '2026-05-22T00:00:00.000Z',
        }),
      ).toBe(true)
    })

    it('returns false when only __mqtCodec is present (no __mqtData)', () => {
      expect(isCodecEnvelope({ __mqtCodec: MessageCodecEnum.ZSTD })).toBe(false)
    })

    it('returns false when only __mqtData is present (no __mqtCodec)', () => {
      expect(isCodecEnvelope({ __mqtData: VALID_BASE64 })).toBe(false)
    })
  })

  describe('invalid __mqtCodec values', () => {
    it('returns false for an unknown codec name', () => {
      expect(isCodecEnvelope({ __mqtCodec: 'gzip', __mqtData: VALID_BASE64 })).toBe(false)
    })

    it('returns false when __mqtCodec is not a string', () => {
      expect(isCodecEnvelope({ __mqtCodec: 42, __mqtData: VALID_BASE64 })).toBe(false)
    })
  })

  describe('invalid __mqtData values — base64 validation', () => {
    it('returns false for a non-base64 string', () => {
      expect(
        isCodecEnvelope({ __mqtCodec: MessageCodecEnum.ZSTD, __mqtData: 'not base64!!!' }),
      ).toBe(false)
    })

    it('returns false when __mqtData is not a string', () => {
      expect(isCodecEnvelope({ __mqtCodec: MessageCodecEnum.ZSTD, __mqtData: 123 })).toBe(false)
    })

    it('returns false for a base64 string with incorrect padding', () => {
      // Valid base64 chars but wrong padding length
      expect(isCodecEnvelope({ __mqtCodec: MessageCodecEnum.ZSTD, __mqtData: 'abc' })).toBe(false)
    })
  })

  describe('non-object inputs', () => {
    it('returns false for null', () => {
      expect(isCodecEnvelope(null)).toBe(false)
    })

    it('returns false for a string', () => {
      expect(isCodecEnvelope('{"__mqtCodec":"zstd","__mqtData":""}')).toBe(false)
    })

    it('returns false for a number', () => {
      expect(isCodecEnvelope(42)).toBe(false)
    })

    it('returns false for an empty object', () => {
      expect(isCodecEnvelope({})).toBe(false)
    })
  })
})

describe('hasCodecEnvelopeShape', () => {
  it('returns true for an envelope naming a codec unknown to this consumer', () => {
    // Structural check — unlike isCodecEnvelope it does not consult a knownCodecs set,
    // so an envelope for an unregistered codec is still recognised (and can be surfaced
    // as a misconfiguration rather than processed as an incomplete message).
    expect(hasCodecEnvelopeShape({ __mqtCodec: 'lz4', __mqtData: VALID_BASE64 })).toBe(true)
  })

  it('returns true for an envelope carrying preserved sibling fields', () => {
    expect(
      hasCodecEnvelopeShape({
        __mqtCodec: MessageCodecEnum.ZSTD,
        __mqtData: VALID_BASE64,
        id: 'm-1',
        type: 'permissions.add',
      }),
    ).toBe(true)
  })

  it('returns false when __mqtCodec is an empty string', () => {
    expect(hasCodecEnvelopeShape({ __mqtCodec: '', __mqtData: VALID_BASE64 })).toBe(false)
  })

  it('returns false when a marker field is missing', () => {
    expect(hasCodecEnvelopeShape({ __mqtCodec: 'lz4' })).toBe(false)
    expect(hasCodecEnvelopeShape({ __mqtData: VALID_BASE64 })).toBe(false)
  })

  it('returns false when __mqtData is not valid base64', () => {
    expect(hasCodecEnvelopeShape({ __mqtCodec: 'lz4', __mqtData: 'not base64!!!' })).toBe(false)
  })

  it('returns false for non-object inputs', () => {
    expect(hasCodecEnvelopeShape(null)).toBe(false)
    expect(hasCodecEnvelopeShape('a string')).toBe(false)
  })
})
