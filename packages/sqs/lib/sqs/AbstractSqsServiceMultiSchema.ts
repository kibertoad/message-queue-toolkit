import {SQSCreationConfig} from "./AbstractSqsConsumer";
import {
    AbstractQueueServiceMultiSchema,
    ExistingQueueOptionsMultiSchema,
    NewQueueOptionsMultiSchema
} from "@message-queue-toolkit/core";
import {SQSClient} from "@aws-sdk/client-sqs";
import {initSqs} from "./sqsInitter";
import {SQSDependencies, SQSQueueLocatorType} from "./AbstractSqsService";

export class AbstractSqsServiceMultiSchema<
    MessagePayloadType extends object,
    QueueLocatorType extends SQSQueueLocatorType = SQSQueueLocatorType,
    CreationConfigType extends SQSCreationConfig = SQSCreationConfig,
    SQSOptionsType extends
            | NewQueueOptionsMultiSchema<MessagePayloadType, CreationConfigType>
        | ExistingQueueOptionsMultiSchema<MessagePayloadType, QueueLocatorType> =
            | NewQueueOptionsMultiSchema<MessagePayloadType, CreationConfigType>
        | ExistingQueueOptionsMultiSchema<MessagePayloadType, QueueLocatorType>,
    DependenciesType extends SQSDependencies = SQSDependencies,
> extends AbstractQueueServiceMultiSchema<
    MessagePayloadType,
    DependenciesType,
    CreationConfigType,
    QueueLocatorType,
    SQSOptionsType
> {
    protected readonly sqsClient: SQSClient
    // @ts-ignore
    public queueUrl: string
    // @ts-ignore
    public queueName: string

    constructor(dependencies: DependenciesType, options: SQSOptionsType) {
        super(dependencies, options)

        this.sqsClient = dependencies.sqsClient
    }

    public async init() {
        const { queueUrl, queueName } = await initSqs(this.sqsClient, this.locatorConfig, this.creationConfig)

        this.queueUrl = queueUrl
        this.queueName = queueName
    }

    // eslint-disable-next-line @typescript-eslint/require-await
    public override async close(): Promise<void> {}
}
