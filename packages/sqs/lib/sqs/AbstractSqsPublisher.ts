import type {AsyncPublisher} from '@message-queue-toolkit/core'
import {AbstractSqsService} from "./AbstractSqsService";
import {SendMessageCommand, SendMessageCommandInput} from "@aws-sdk/client-sqs"
import {string} from "zod";

export type SQSMessageOptions = {
  MessageGroupId?: string
  MessageDeduplicationId?: string
}

export abstract class AbstractSqsPublisher<MessagePayloadType extends {}>
  extends AbstractSqsService<MessagePayloadType>
  implements AsyncPublisher<MessagePayloadType, SQSMessageOptions>
{
  async publish(message: MessagePayloadType, options: SQSMessageOptions = {}): Promise<void> {
    try {
      const input = { // SendMessageRequest
        QueueUrl: this.queueName,
        MessageBody: JSON.stringify(message),
        ...options
      } satisfies SendMessageCommandInput;
      const command = new SendMessageCommand(input);
      await this.sqsClient.send(command);
    } catch (error) {
      this.handleError(error)
      throw error
    }
  }
}
