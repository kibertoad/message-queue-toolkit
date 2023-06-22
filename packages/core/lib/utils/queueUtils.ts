export const objectToBuffer = <T extends object>(object: T): Buffer => {
  return Buffer.from(JSON.stringify(object))
}
