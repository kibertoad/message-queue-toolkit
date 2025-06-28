import z from 'zod/v3'

export type CommonMessage = {
  messageType: string
}

export const SNS_MESSAGE_BODY_SCHEMA = z.object({
  Type: z.string(),
  MessageId: z.string(),
  TopicArn: z.string(),
  Message: z.string(),
  MessageAttributes: z
    .record(
      z.string(),
      z.object({
        Type: z.string(),
        Value: z.any(),
      }),
    )
    .optional(),
  Timestamp: z.string(),
  SignatureVersion: z.string(),
  Signature: z.string(),
  SigningCertURL: z.string(),
  UnsubscribeURL: z.string(),
})

export type SNS_MESSAGE_BODY_TYPE = z.infer<typeof SNS_MESSAGE_BODY_SCHEMA>
