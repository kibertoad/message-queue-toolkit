import type { ProcessedMessageMetadata } from '@message-queue-toolkit/core'

/**
 * Parameters used for registering message processing metrics in Prometheus
 */
export type PrometheusMetricParams<T extends object> = {
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
  buckets?: number[]

  /**
   * Message version used as a label - can be static string or method resolving version based on payload
   */
  messageVersion?: string | MessageVersionGeneratingFunction<T>
}

export type MessageVersionGeneratingFunction<T extends object> = (
  messageMetadata: ProcessedMessageMetadata<T>,
) => string | undefined
