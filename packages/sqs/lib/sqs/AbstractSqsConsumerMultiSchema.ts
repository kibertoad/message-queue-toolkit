import {SQSConsumerDependencies, SQSQueueLocatorType} from "./AbstractSqsService";
import {SQSMessage} from "../types/MessageTypes";
import {
    AbstractSqsConsumer,
    ExistingSQSConsumerOptions,
    NewSQSConsumerOptions,
    SQSCreationConfig
} from "./AbstractSqsConsumer";
import {ZodSchema} from "zod";
import {
    HandlerContainer,
    MessageSchemaContainer,
} from "@message-queue-toolkit/core";
import {Either} from "@lokalise/node-core";

export abstract class AbstractSqsConsumerMultiSchema<
    MessagePayloadType extends object,
    QueueLocatorType extends SQSQueueLocatorType = SQSQueueLocatorType,
    CreationConfigType extends SQSCreationConfig = SQSCreationConfig,
    ConsumerOptionsType extends
            | NewSQSConsumerOptions<MessagePayloadType, CreationConfigType>
        | ExistingSQSConsumerOptions<MessagePayloadType, QueueLocatorType> =
            | NewSQSConsumerOptions<MessagePayloadType, CreationConfigType>
        | ExistingSQSConsumerOptions<MessagePayloadType, QueueLocatorType>,
> extends AbstractSqsConsumer<MessagePayloadType, QueueLocatorType, CreationConfigType, ConsumerOptionsType> {

    messageSchemaContainer: MessageSchemaContainer<MessagePayloadType>
    handlerContainer: HandlerContainer<MessagePayloadType, typeof this>


    constructor(dependencies: SQSConsumerDependencies, options: ConsumerOptionsType, multiSchemaOptions: MultiSchemaConsumerOptionsZ) {
        super(dependencies, options);

        const messageSchemas = multiSchemaOptions.handlers.map((entry) => entry.schema)

        this.messageSchemaContainer = new MessageSchemaContainer<MessagePayloadType>({
            messageSchemas,
            messageTypeField: options.messageTypeField
        });
        this.handlerContainer = new HandlerContainer<MessagePayloadType, typeof this>({
            messageTypeField: this.messageTypeField,
            messageHandlers: multiSchemaOptions.handlers
        })
    }

    protected override resolveSchema(message: SQSMessage): ZodSchema<MessagePayloadType> {
        return this.messageSchemaContainer.resolveSchema(JSON.parse(message.Body))
    }

    protected override async processMessage(message: MessagePayloadType, messageType: string): Promise<Either<"retryLater", "success">> {
        const handler = this.handlerContainer.resolveHandler(messageType)
        return handler(message, this)
    }
}
