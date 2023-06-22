export const objectToBuffer = <T extends Record<string, unknown>>(object: T): Buffer => {
  return Buffer.from(JSON.stringify(object))
}
