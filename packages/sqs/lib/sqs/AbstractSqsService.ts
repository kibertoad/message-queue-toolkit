import type { SQSClient } from "@aws-sdk/client-sqs";
import { CreateQueueCommand } from "@aws-sdk/client-sqs";

import {AbstractQueueService, QueueDependencies, QueueOptions} from "@message-queue-toolkit/core";

export type SQSDependencies = QueueDependencies & {
    sqsClient: SQSClient
}

export class AbstractSqsService<MessagePayloadType extends {}, SQSOptionsType extends QueueOptions<MessagePayloadType> = QueueOptions<MessagePayloadType>> extends AbstractQueueService<MessagePayloadType, SQSDependencies, SQSOptionsType>{
    protected readonly sqsClient: SQSClient

    constructor(
        dependencies: SQSDependencies,
        options: SQSOptionsType,
    ) {
        super(dependencies, options)

        this.sqsClient = dependencies.sqsClient
    }

    public async init() {
        const command = new CreateQueueCommand({
            QueueName: this.queueName
        })
        await this.sqsClient.send(command)
    }

    async close(): Promise<void> {
        this.sqsClient.destroy()
    }
}
