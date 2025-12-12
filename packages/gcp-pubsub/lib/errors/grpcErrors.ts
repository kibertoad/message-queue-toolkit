import { status as GrpcStatus } from '@grpc/grpc-js'

/**
 * gRPC status codes for which subscription operations should be retried.
 *
 * Includes both:
 * 1. GCP-documented retryable errors (DEADLINE_EXCEEDED, RESOURCE_EXHAUSTED, INTERNAL, UNAVAILABLE)
 * 2. Eventual consistency errors common after Terraform deployments (NOT_FOUND, PERMISSION_DENIED)
 *
 * **Why PERMISSION_DENIED is included:**
 * After Terraform deployments, IAM permissions can take several minutes to propagate across
 * GCP's distributed infrastructure. During this window, the subscription may report
 * PERMISSION_DENIED even though permissions are correctly configured.
 *
 * **Why NOT_FOUND is included:**
 * Similar to PERMISSION_DENIED, newly created subscriptions may not be immediately visible
 * across all GCP endpoints due to eventual consistency.
 *
 * @see https://cloud.google.com/pubsub/docs/reference/error-codes
 * @see https://github.com/googleapis/nodejs-pubsub/issues/979
 */
export const RETRYABLE_GRPC_STATUS_CODES = [
  GrpcStatus.DEADLINE_EXCEEDED,
  GrpcStatus.NOT_FOUND,
  GrpcStatus.PERMISSION_DENIED,
  GrpcStatus.RESOURCE_EXHAUSTED,
  GrpcStatus.INTERNAL,
  GrpcStatus.UNAVAILABLE,
] as const

export type RetryableGrpcStatusCode = (typeof RETRYABLE_GRPC_STATUS_CODES)[number]

const RETRYABLE_CODES_SET = new Set<number>(RETRYABLE_GRPC_STATUS_CODES)

/**
 * Type for errors with a numeric gRPC status code and message.
 */
export type GrpcError = {
  code: number
  message: string
}

/**
 * Checks if an error has gRPC error properties (code and message).
 * Uses duck typing to avoid fragile instanceof checks.
 *
 * @param error - The error to check
 * @returns true if the error has numeric `code` and string `message` properties
 */
export function isGrpcError(error: unknown): error is GrpcError {
  return (
    typeof error === 'object' &&
    error !== null &&
    'code' in error &&
    'message' in error &&
    typeof (error as GrpcError).code === 'number' &&
    typeof (error as GrpcError).message === 'string'
  )
}

/**
 * Checks if an error is a gRPC error with a retryable status code.
 *
 * @param error - The error to check
 * @returns true if the error has a retryable gRPC status code
 */
export function isRetryableGrpcError(error: unknown): error is GrpcError {
  return isGrpcError(error) && RETRYABLE_CODES_SET.has(error.code)
}

/**
 * Gets the gRPC status code from an error if it has one.
 *
 * @param error - The error to extract the code from
 * @returns The gRPC status code, or undefined if not a gRPC error
 */
export function getGrpcStatusCode(error: unknown): number | undefined {
  if (isGrpcError(error)) {
    return error.code
  }
  return undefined
}

// Re-export for convenience
export { GrpcStatus }
