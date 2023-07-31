import {SQSConsumerDependencies, SQSQueueLocatorType} from "./AbstractSqsService";
import {SQSMessage} from "../types/MessageTypes";
import {
    AbstractSqsConsumer,
    SQSCreationConfig
} from "./AbstractSqsConsumer";
import {ZodSchema} from "zod";
import {
    Deserializer, ExistingQueueOptionsMultiSchema,
    HandlerContainer,
    MessageSchemaContainer, NewQueueOptionsMultiSchema,
} from "@message-queue-toolkit/core";
import {Either} from "@lokalise/node-core";
import {ConsumerOptions} from "sqs-consumer/src/types";

export type NewSQSConsumerOptionsMultiSchema<
    MessagePayloadSchemas extends object,
    ExecutionContext,
    CreationConfigType extends SQSCreationConfig,
> = NewQueueOptionsMultiSchema<MessagePayloadSchemas, CreationConfigType, ExecutionContext> & {
    consumerOverrides?: Partial<ConsumerOptions>
    deserializer?: Deserializer<MessagePayloadSchemas, SQSMessage>
}

export type ExistingSQSConsumerOptionsMultiSchema<
    MessagePayloadSchemas extends object,
    ExecutionContext,
    QueueLocatorType extends SQSQueueLocatorType = SQSQueueLocatorType,
> = ExistingQueueOptionsMultiSchema<MessagePayloadSchemas, QueueLocatorType, ExecutionContext> & {
    consumerOverrides?: Partial<ConsumerOptions>
    deserializer?: Deserializer<MessagePayloadSchemas, SQSMessage>
}

export abstract class AbstractSqsConsumerMultiSchema<
    MessagePayloadType extends object,
    ExecutionContext,
    QueueLocatorType extends SQSQueueLocatorType = SQSQueueLocatorType,
    CreationConfigType extends SQSCreationConfig = SQSCreationConfig,
    ConsumerOptionsType extends
            | NewSQSConsumerOptionsMultiSchema<MessagePayloadType, ExecutionContext, CreationConfigType>
        | ExistingSQSConsumerOptionsMultiSchema<MessagePayloadType, ExecutionContext, QueueLocatorType> =
            | NewSQSConsumerOptionsMultiSchema<MessagePayloadType, ExecutionContext, CreationConfigType>
        | ExistingSQSConsumerOptionsMultiSchema<MessagePayloadType, ExecutionContext, QueueLocatorType>,
> extends AbstractSqsConsumer<MessagePayloadType, QueueLocatorType, CreationConfigType, ConsumerOptionsType> {

    messageSchemaContainer: MessageSchemaContainer<MessagePayloadType>
    handlerContainer: HandlerContainer<MessagePayloadType, ExecutionContext>


    constructor(dependencies: SQSConsumerDependencies, options: ConsumerOptionsType) {
        super(dependencies, options);

        const messageSchemas = options.handlers.map((entry) => entry.schema)

        this.messageSchemaContainer = new MessageSchemaContainer<MessagePayloadType>({
            messageSchemas,
            messageTypeField: options.messageTypeField
        });
        this.handlerContainer = new HandlerContainer<MessagePayloadType, ExecutionContext>({
            messageTypeField: this.messageTypeField,
            messageHandlers: options.handlers
        })
    }

    protected override resolveSchema(message: SQSMessage): ZodSchema<MessagePayloadType> {
        return this.messageSchemaContainer.resolveSchema(JSON.parse(message.Body))
    }

    public override async processMessage(message: MessagePayloadType, messageType: string): Promise<Either<"retryLater", "success">> {
        const handler = this.handlerContainer.resolveHandler(messageType)
        // @ts-ignore
        return handler(message, this)
    }
}
