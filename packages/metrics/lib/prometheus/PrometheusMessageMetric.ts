import type { MessageMetricsManager, ProcessedMessageMetadata } from '@message-queue-toolkit/core'
import type { Metric } from 'prom-client'
import promClient from 'prom-client'
import type { MessageVersionGeneratingFunction, PrometheusMetricParams } from './types.ts'

/**w
 * Implementation of MessageMetricsManager for Prometheus
 */
export abstract class PrometheusMessageMetric<
  MessagePayload extends object,
  MetricType extends Metric,
  MetricParams extends
    PrometheusMetricParams<MessagePayload> = PrometheusMetricParams<MessagePayload>,
> implements MessageMetricsManager<MessagePayload>
{
  /** Fallbacks to null if metrics are disabled on app level */
  protected readonly metric: MetricType

  protected readonly messageVersionGeneratingFunction: MessageVersionGeneratingFunction<MessagePayload>

  private readonly metricParams: MetricParams

  /**
   * @param metricParams - metrics parameters (see PrometheusMetricParams)
   * @param client - use it to specify custom Prometheus client
   */
  constructor(metricParams: MetricParams, client?: typeof promClient) {
    this.metricParams = metricParams
    this.messageVersionGeneratingFunction =
      this.resolveMessageVersionGeneratingFunction(metricParams)
    this.metric = this.registerMetric(client ?? promClient)
  }

  private registerMetric(client: typeof promClient): MetricType {
    const existingMetric = client.register.getSingleMetric(this.metricParams.name)

    return existingMetric
      ? (existingMetric as MetricType)
      : this.createMetric(client, this.metricParams)
  }

  private resolveMessageVersionGeneratingFunction(
    metricParams: MetricParams,
  ): MessageVersionGeneratingFunction<MessagePayload> {
    const messageVersion = metricParams.messageVersion
    return typeof messageVersion === 'function' ? messageVersion : () => messageVersion
  }

  protected abstract createMetric(client: typeof promClient, metricParams: MetricParams): MetricType
  abstract registerProcessedMessage(metadata: ProcessedMessageMetadata<MessagePayload>): void
}
