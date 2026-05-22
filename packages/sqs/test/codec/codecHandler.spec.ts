import { getCodecName, MessageCodecEnum } from '@message-queue-toolkit/core'
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
