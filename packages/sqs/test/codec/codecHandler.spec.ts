import { decompressMessageBody, getCodecName, MessageCodecEnum } from '@message-queue-toolkit/core'
import { describe, expect, it } from 'vitest'

describe('getCodecName', () => {
  it('returns the string as-is for built-in codec enum values', () => {
    expect(getCodecName(MessageCodecEnum.ZSTD)).toBe('zstd')
  })

  it('returns the name for a valid custom codec registration', () => {
    expect(getCodecName({ name: 'lz4', handler: {} as any })).toBe('lz4')
    expect(getCodecName({ name: 'my-codec_v2', handler: {} as any })).toBe('my-codec_v2')
  })

  it('throws for a custom codec name containing a double-quote', () => {
    expect(() => getCodecName({ name: 'lz4"x', handler: {} as any })).toThrow(
      'Invalid codec name "lz4"x"',
    )
  })

  it('throws for a custom codec name containing a backslash', () => {
    expect(() => getCodecName({ name: 'lz4\\x', handler: {} as any })).toThrow(
      'Invalid codec name "lz4\\x"',
    )
  })

  it('throws for a custom codec name containing whitespace', () => {
    expect(() => getCodecName({ name: 'my codec', handler: {} as any })).toThrow(
      'Invalid codec name "my codec"',
    )
  })

  it('throws for an empty custom codec name', () => {
    expect(() => getCodecName({ name: '', handler: {} as any })).toThrow('Invalid codec name ""')
  })
})

describe('decompressMessageBody', () => {
  it('throws a descriptive error when __mqtData is not valid base64', async () => {
    await expect(
      decompressMessageBody({ __mqtCodec: MessageCodecEnum.ZSTD, __mqtData: 'not-base64!!!' }),
    ).rejects.toThrow('Codec envelope __mqtData is not valid base64 (codec: zstd)')
  })

  it('throws a descriptive error for base64 with incorrect padding', async () => {
    // Valid base64 characters but wrong padding — Buffer.from would silently accept this
    // and produce garbage bytes; the guard must catch it before the codec is invoked.
    await expect(
      decompressMessageBody({ __mqtCodec: MessageCodecEnum.ZSTD, __mqtData: 'abc' }),
    ).rejects.toThrow('Codec envelope __mqtData is not valid base64 (codec: zstd)')
  })
})
