import { stringValueSerializer } from '@lokalise/node-core'
import { safeJsonDeserializer } from './safeJsonDeserializer.ts'

describe('safeJsonDeserializer', () => {
  it('should deserialize valid JSON strings', () => {
    const validJson = JSON.stringify({ key: 'value' })
    const result = safeJsonDeserializer(validJson)
    expect(result).toEqual({ key: 'value' })
  })

  it('should return undefined for invalid JSON strings', () => {
    const invalidJson = stringValueSerializer({ key: 'value' })
    const result = safeJsonDeserializer(invalidJson)
    expect(result).toBeUndefined()
  })

  it('should deserialize for valid buffer inputs', () => {
    const buffer = Buffer.from(JSON.stringify({ key: 'value' }))
    const result = safeJsonDeserializer(buffer)
    expect(result).toEqual({ key: 'value' })
  })

  it('should return undefined for invalid buffer inputs', () => {
    const invalidBuffer = Buffer.from('invalid json')
    const result = safeJsonDeserializer(invalidBuffer)
    expect(result).toBeUndefined()
  })

  it('should return undefined for invalid inputs', () => {
    const result = safeJsonDeserializer(1 as any)
    expect(result).toBeUndefined()
  })

  it('should return undefined for undefined inputs', () => {
    const result = safeJsonDeserializer(undefined)
    expect(result).toBeUndefined()
  })
})
