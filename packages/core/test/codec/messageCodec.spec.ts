import { describe, expect, it } from 'vitest'

import { isCodecEnvelope, MessageCodecEnum } from '../../lib/codec/messageCodec.ts'

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

  describe('extra fields — real messages must not be misclassified', () => {
    it('returns false when envelope has an extra field alongside __mqtCodec and __mqtData', () => {
      expect(
        isCodecEnvelope({
          __mqtCodec: MessageCodecEnum.ZSTD,
          __mqtData: VALID_BASE64,
          id: 'real-message',
        }),
      ).toBe(false)
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
