import {assertQueue, getQueueAttributes} from "../utils/SqsUtils";
import {SQSClient} from "@aws-sdk/client-sqs";
import {SQSQueueLocatorType} from "./AbstractSqsService";
import {SQSCreationConfig} from "./AbstractSqsConsumer";

export async function initSqs(sqsClient: SQSClient,
                        locatorConfig?: SQSQueueLocatorType,
                        creationConfig?: SQSCreationConfig,
) {
    // reuse existing queue only
    if (locatorConfig) {
        const checkResult = await getQueueAttributes(sqsClient, locatorConfig)
        if (checkResult.error === 'not_found') {
            throw new Error(`Queue with queueUrl ${locatorConfig.queueUrl} does not exist.`)
        }

        const queueUrl = locatorConfig.queueUrl

        const splitUrl = queueUrl.split('/')
        const queueName = splitUrl[splitUrl.length - 1]
        return {
            queueUrl,
            queueName
        }
    }

    // create new queue if does not exist
    if (!creationConfig?.queue.QueueName) {
        throw new Error('queueConfig.QueueName is mandatory when locator is not provided')
    }

    const queueUrl = await assertQueue(sqsClient, creationConfig.queue)
    const queueName = creationConfig.queue.QueueName

    return {
        queueUrl,
        queueName,
    }
}
