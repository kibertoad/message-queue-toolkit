import type { MessageAttributeValue, MessageSystemAttributeName } from '@aws-sdk/client-sqs'

export type SQSMessage = {
  MessageId: string
  ReceiptHandle: string
  MD5OfBody: string
  Body: string
  MessageAttributes?: Record<string, MessageAttributeValue>
  Attributes?: Partial<Record<MessageSystemAttributeName, string>>
}

export type CommonMessage = {
  messageType: string
}
