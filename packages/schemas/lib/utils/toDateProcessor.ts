export const toDatePreprocessor = (value: unknown) => {
  switch (typeof value) {
    case 'string':
    case 'number':
      return new Date(value)

    default:
      return value // could not coerce, return the original and face the consequences during validation
  }
}
