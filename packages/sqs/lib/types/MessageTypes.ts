export type SQSMessage = {
  MessageId: string
  ReceiptHandle: string
  MD5OfBody: string
  Body: string
}

export type CommonMessage = {
  messageType: string
}
