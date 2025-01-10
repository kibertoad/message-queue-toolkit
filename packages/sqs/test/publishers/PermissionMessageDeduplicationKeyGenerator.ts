import type { MessageDeduplicationKeyGenerator } from '@message-queue-toolkit/core'
import type {
  PERMISSIONS_ADD_MESSAGE_TYPE,
  PERMISSIONS_MESSAGE_TYPE,
  PERMISSIONS_REMOVE_MESSAGE_TYPE,
} from '../consumers/userConsumerSchemas'

type PermissionMessagesType =
  | PERMISSIONS_MESSAGE_TYPE
  | PERMISSIONS_ADD_MESSAGE_TYPE
  | PERMISSIONS_REMOVE_MESSAGE_TYPE

export class PermissionMessageDeduplicationKeyGenerator
  implements MessageDeduplicationKeyGenerator<PermissionMessagesType>
{
  generate(message: PermissionMessagesType): string {
    return message.id
  }
}
