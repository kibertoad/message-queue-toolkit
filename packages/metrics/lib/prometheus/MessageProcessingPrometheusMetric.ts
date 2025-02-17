import type { MessageMetricsManager, ProcessedMessageMetadata } from '@message-queue-toolkit/core'
import type { Histogram } from 'prom-client'
import promClient from 'prom-client'

/**
 * Parameters used for registering message processing metrics in Prometheus
 */
type PrometheusMetricParams<T extends object> = {
  /**
   * Prometheus metric name
   */
  name: string

  /**
   * Prometheus metric description
   */
  helpDescription: string

  /**
   * Buckets used to configure Histogram metric
   */
  buckets: number[]

  /**
   * Message version used as a label - can be static string or method resolving version based on payload
   */
  messageVersion?: string | MessageVersionGeneratingFunction<T>
}

type MessageVersionGeneratingFunction<T extends object> = (
  messageMetadata: ProcessedMessageMetadata<T>,
) => string | undefined

/**w
 * Implementation of MessageMetricsManager that can be used to register message processing measurements in Prometheus utilizing Histogram
 */
export abstract class MessageProcessingPrometheusMetric<MessagePayloadSchemas extends object>
  implements MessageMetricsManager<MessagePayloadSchemas>
{
  protected readonly metricParams: PrometheusMetricParams<MessagePayloadSchemas>

  /** Fallbacks to null if metrics are disabled on app level */
  protected readonly metric: Histogram<'messageType' | 'version' | 'queue' | 'result'>

  protected readonly messageVersionGeneratingFunction: MessageVersionGeneratingFunction<MessagePayloadSchemas>

  /**
   * @param metricParams - metrics parameters (see PrometheusMetricParams)
   * @param client - use it to specify custom Prometheus client
   */
  constructor(
    metricParams: PrometheusMetricParams<MessagePayloadSchemas>,
    client?: typeof promClient,
  ) {
    this.metricParams = metricParams
    this.messageVersionGeneratingFunction =
      this.resolveMessageVersionGeneratingFunction(metricParams)
    this.metric = this.registerMetric(client ?? promClient)
  }

  protected abstract calculateObservedValue(
    metadata: ProcessedMessageMetadata<MessagePayloadSchemas>,
  ): number | null

  registerProcessedMessage(metadata: ProcessedMessageMetadata<MessagePayloadSchemas>): void {
    const observedValue: number | null = this.calculateObservedValue(metadata)

    if (observedValue === null) {
      // Data not available, skipping
      return
    }

    this.metric.observe(
      {
        messageType: metadata.messageType,
        version: this.messageVersionGeneratingFunction(metadata),
        queue: metadata.queueName,
        result: metadata.processingResult,
      },
      observedValue,
    )
  }

  protected registerMetric(client: typeof promClient): Histogram {
    const existingMetric = client.register.getSingleMetric(this.metricParams.name)

    if (existingMetric) {
      return existingMetric as Histogram
    }

    return new client.Histogram({
      name: this.metricParams.name,
      help: this.metricParams.helpDescription,
      buckets: this.metricParams.buckets,
      labelNames: ['messageType', 'version', 'queue', 'result'],
    })
  }

  protected resolveMessageVersionGeneratingFunction(
    metricParams: PrometheusMetricParams<MessagePayloadSchemas>,
  ): MessageVersionGeneratingFunction<MessagePayloadSchemas> {
    const messageVersion = metricParams.messageVersion
    return typeof messageVersion === 'function' ? messageVersion : () => messageVersion
  }
}
