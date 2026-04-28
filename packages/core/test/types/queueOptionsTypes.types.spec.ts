/**
 * Type-level tests for `CommonQueueOptions`. These guard against silent removals of legacy
 * options. The lib historically exposed `messageTypeField`, removed in core 25.x in favor
 * of `messageTypeResolver`. Callers that still pass the legacy option must get a compile-time
 * error rather than silently broken runtime behavior (for example: payload offloading drops
 * the message `type` field, then SNS subscriptions with `FilterPolicyScope: 'MessageBody'`
 * filter on `type` fail to deliver to SQS).
 *
 * Run with: npx tsc --noEmit
 */
import { describe, it } from 'vitest'
import type { CommonQueueOptions } from '../../lib/types/queueOptionsTypes.ts'

describe('CommonQueueOptions — legacy `messageTypeField`', () => {
  it('rejects passing the removed `messageTypeField` option', () => {
    const _bad: CommonQueueOptions = {
      // @ts-expect-error - `messageTypeField` was removed in core 25.x; use `messageTypeResolver` instead
      messageTypeField: 'type',
    }
  })

  it('accepts the supported `messageTypeResolver` shape', () => {
    const _ok1: CommonQueueOptions = { messageTypeResolver: { messageTypePath: 'type' } }
    const _ok2: CommonQueueOptions = { messageTypeResolver: { literal: 'order.created' } }
    const _ok3: CommonQueueOptions = {
      messageTypeResolver: { resolver: () => 'order.created' },
    }
  })
})
