import { describe, expect, it } from 'vitest'

import { buildCodecEnvelope } from '../../lib/codec/codecHandler.ts'
import { hasCodecEnvelopeShape, isCodecEnvelope } from '../../lib/codec/messageCodec.ts'

const COMPRESSED = Buffer.from('compressed-bytes')
const COMPRESSED_B64 = COMPRESSED.toString('base64')

describe('buildCodecEnvelope', () => {
  it('emits a two-field envelope when no preserved fields are given', () => {
    expect(JSON.parse(buildCodecEnvelope(COMPRESSED, 'zstd'))).toEqual({
      __mqtCodec: 'zstd',
      __mqtData: COMPRESSED_B64,
    })
  })

  it('emits a two-field envelope when preserved fields are an empty object', () => {
    expect(JSON.parse(buildCodecEnvelope(COMPRESSED, 'zstd', {}))).toEqual({
      __mqtCodec: 'zstd',
      __mqtData: COMPRESSED_B64,
    })
  })

  it('emits preserved fields as plaintext siblings of the codec fields', () => {
    const envelope = JSON.parse(
      buildCodecEnvelope(COMPRESSED, 'zstd', { id: 'm-1', type: 'permissions.add' }),
    )
    expect(envelope).toEqual({
      id: 'm-1',
      type: 'permissions.add',
      __mqtCodec: 'zstd',
      __mqtData: COMPRESSED_B64,
    })
  })

  it('escapes preserved field values that contain JSON metacharacters', () => {
    const envelope = JSON.parse(
      buildCodecEnvelope(COMPRESSED, 'zstd', { id: 'quote " and \\ backslash' }),
    )
    expect(envelope.id).toBe('quote " and \\ backslash')
    expect(envelope.__mqtData).toBe(COMPRESSED_B64)
  })

  it('codec fields always win over a colliding preserved field name', () => {
    const envelope = JSON.parse(
      buildCodecEnvelope(COMPRESSED, 'zstd', { __mqtData: 'tampered', __mqtCodec: 'tampered' }),
    )
    expect(envelope.__mqtData).toBe(COMPRESSED_B64)
    expect(envelope.__mqtCodec).toBe('zstd')
  })

  it('produces an envelope still recognised by isCodecEnvelope and hasCodecEnvelopeShape', () => {
    const envelope = JSON.parse(
      buildCodecEnvelope(COMPRESSED, 'zstd', { id: 'm-1', type: 'permissions.add' }),
    )
    expect(isCodecEnvelope(envelope)).toBe(true)
    expect(hasCodecEnvelopeShape(envelope)).toBe(true)
  })
})
