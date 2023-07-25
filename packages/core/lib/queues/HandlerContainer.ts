import {Either} from "@lokalise/node-core";
import {ZodSchema} from "zod";

export class MessageHandlerConfig<ExecutionContext, MessagePayloadSchemas> {
    public readonly schema: ZodSchema<MessagePayloadSchemas>;
    public readonly handler: Handler<MessagePayloadSchemas, ExecutionContext>;

    constructor (
        schema: ZodSchema<MessagePayloadSchemas>,
        handler: Handler<MessagePayloadSchemas, ExecutionContext>
    ) {
        this.schema = schema
        this.handler = handler
    }
}

export type Handler<MessagePayloadSchemas, ExecutionContext> = (message: MessagePayloadSchemas, context: ExecutionContext) => Promise<Either<'retryLater', 'success'>>

export type HandlerContainerOptions<MessagePayloadSchemas extends object, ExecutionContext> = {
    messageHandlers: MessageHandlerConfig<MessagePayloadSchemas, ExecutionContext>[]
    messageTypeField: string
}

export class HandlerContainer<MessagePayloadSchemas extends object, ExecutionContext> {
    private readonly messageHandlers: Record<string, Handler<MessagePayloadSchemas, ExecutionContext>>
    private readonly messageTypeField: string;

    constructor(options: HandlerContainerOptions<MessagePayloadSchemas, ExecutionContext>) {
        this.messageTypeField = options.messageTypeField
        this.messageHandlers = this.resolveHandlerMap(options.messageHandlers)
    }

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    public resolveHandler(messageType: string): Handler<MessagePayloadSchemas, ExecutionContext> {
        const handler = this.messageHandlers[messageType]
        if (!handler) {
            throw new Error(`Unsupported message type: ${messageType}`)
        }
        return handler
    }

    private resolveHandlerMap(
        supportedHandlers: MessageHandlerConfig<MessagePayloadSchemas, ExecutionContext>[],
    ): Record<string, Handler<MessagePayloadSchemas, ExecutionContext>> {
        return supportedHandlers.reduce(
            (acc, entry) => {
                // @ts-ignore
                const messageType = entry.schema.shape[this.messageType].value
                acc[messageType] = entry.handler
                return acc
            },
            {} as Record<string, Handler<MessagePayloadSchemas, ExecutionContext>>,
        )
    }
}
