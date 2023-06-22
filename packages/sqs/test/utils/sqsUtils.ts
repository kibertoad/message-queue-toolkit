import {DeleteQueueCommand, SQSClient} from "@aws-sdk/client-sqs";

export async function deleteQueue(client: SQSClient, queueName: string) {
    const command = new DeleteQueueCommand({
        QueueUrl: queueName
    })

    await client.send(command)
}
