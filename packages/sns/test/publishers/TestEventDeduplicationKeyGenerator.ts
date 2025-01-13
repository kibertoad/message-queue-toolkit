import type { MessageDeduplicationKeyGenerator } from '@message-queue-toolkit/core'
import type { TestEventPublishPayloadsType } from '../utils/testContext'

export class TestEventDeduplicationKeyGenerator
  implements MessageDeduplicationKeyGenerator<TestEventPublishPayloadsType>
{
  generate(message: TestEventPublishPayloadsType): string {
    return `${message.type}:${message.payload.entityId}`
  }
}
