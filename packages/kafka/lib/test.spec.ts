import { features, librdkafkaVersion } from 'node-rdkafka'

// TODO: to be removed once we have proper tests
describe('Checking node-rdkafka', () => {
  it('should work', () => {
    expect(features).toMatchInlineSnapshot(`
      [
        "gzip",
        "snappy",
        "sasl",
        "regex",
        "lz4",
        "sasl_gssapi",
        "sasl_plain",
        "plugins",
        "http",
      ]
    `)
    expect(librdkafkaVersion).toMatchInlineSnapshot(`"2.8.0"`)
  })
})
