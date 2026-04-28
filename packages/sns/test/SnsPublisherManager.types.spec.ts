/**
 * Type-level guard for `SnsPublisherManager`. Reproduces the call shape that file-storage-service
 * was using when payload offloading silently broke after the core 24.x → 25.x bump:
 *
 * ```ts
 * new SnsPublisherManager(deps, {
 *   ...
 *   newPublisherOptions: {
 *     messageTypeField: 'type', // <- removed in core 25.x; ignored at runtime, dropped `type` from offloaded SNS body
 *     ...
 *   },
 * })
 * ```
 *
 * This must now fail at compile time.
 *
 * Run with: npx tsc --noEmit
 */
import { describe, it } from 'vitest'
import type { SNSPublisherOptions } from '../lib/sns/AbstractSnsPublisher.ts'

type AnyMessage = { type: string }

type NewPublisherOptions = Omit<
  SNSPublisherOptions<AnyMessage>,
  'messageSchemas' | 'creationConfig' | 'locatorConfig'
>

describe('SnsPublisherManager `newPublisherOptions` — legacy `messageTypeField`', () => {
  it('rejects the removed `messageTypeField` option', () => {
    const _bad: NewPublisherOptions = {
      // @ts-expect-error - `messageTypeField` was removed in core 25.x; use `messageTypeResolver` instead
      messageTypeField: 'type',
      messageIdField: 'id',
    }
  })

  it('accepts `messageTypeResolver`', () => {
    const _ok: NewPublisherOptions = {
      messageTypeResolver: { messageTypePath: 'type' },
      messageIdField: 'id',
    }
  })
})
