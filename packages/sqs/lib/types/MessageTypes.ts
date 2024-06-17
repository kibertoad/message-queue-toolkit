import type { MessageAttributeValue } from '@aws-sdk/client-sqs'

export type SQSMessage = {
  MessageId: string
  ReceiptHandle: string
  MD5OfBody: string
  Body: string
  MessageAttributes?: Record<string, MessageAttributeValue>
}

export type CommonMessage = {
  messageType: string
}
