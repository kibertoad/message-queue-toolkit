import { PrometheusMessageCounter } from './PrometheusMessageCounter.ts'

export class PrometheusMessageResultCounter<
  MessagePayload extends object,
> extends PrometheusMessageCounter<MessagePayload> {
  protected calculateCount(): number | null {
    return 1
  }
}
