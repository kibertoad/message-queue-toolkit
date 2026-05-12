/**
 * Type-level tests for `CommonQueueOptions`. These guard against silent removals of legacy
 * options. The lib historically exposed `messageTypeField`, removed in core 25.x in favor
 * of `messageTypeResolver`. Callers that still pass the legacy option must get a compile-time
 * error rather than silently broken runtime behavior (for example: payload offloading drops
 * the message `type` field, then SNS subscriptions with `FilterPolicyScope: 'MessageBody'`
 * filter on `type` fail to deliver to SQS).
 */
import { describe, expectTypeOf, it } from 'vitest'
import type { CommonQueueOptions } from '../../lib/types/queueOptionsTypes.ts'

describe('CommonQueueOptions — legacy `messageTypeField`', () => {
  it('rejects passing the removed `messageTypeField` option', () => {
    expectTypeOf<{ messageTypeField: 'type' }>().not.toMatchTypeOf<CommonQueueOptions>()
  })

  it('accepts the supported `messageTypeResolver` shape', () => {
    expectTypeOf<{
      messageTypeResolver: { messageTypePath: 'type' }
    }>().toMatchTypeOf<CommonQueueOptions>()
    expectTypeOf<{
      messageTypeResolver: { literal: 'order.created' }
    }>().toMatchTypeOf<CommonQueueOptions>()
    expectTypeOf<{
      messageTypeResolver: { resolver: () => 'order.created' }
    }>().toMatchTypeOf<CommonQueueOptions>()
  })
})
