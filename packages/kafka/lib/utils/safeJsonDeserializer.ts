export const safeJsonDeserializer = (data?: string | Buffer): object | undefined => {
  if (!data) return undefined
  // Checking to be safe
  if (!Buffer.isBuffer(data) && typeof data !== 'string') return undefined

  try {
    return JSON.parse(data.toString('utf-8'))
  } catch (_) {
    return undefined
  }
}
