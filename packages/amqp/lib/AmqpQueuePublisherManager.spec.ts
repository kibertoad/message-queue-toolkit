import {AwilixContainer} from "awilix";
import {Dependencies, registerDependencies} from "../test/utils/testContext";
import {beforeAll} from "vitest";
import {TEST_AMQP_CONFIG} from "../test/utils/testAmqpConfig";

describe('AmqpQueuePublisherManager', () => {
    describe('publish', () => {
        let diContainer: AwilixContainer<Dependencies>
        beforeAll(async () => {
            diContainer = await registerDependencies(TEST_AMQP_CONFIG)
        })

        it('publishes to the correct queue', async () => {
            const { queuePublisherManager } = diContainer.cradle

            await queuePublisherManager.publish('dummy', {
                ...queuePublisherManager.resolveBaseFields(),
                type: 'entity.updated',
                payload: {
                    updatedData: 'msg'
                }
            })

            const result = await queuePublisherManager.handlerSpy('dummy').waitForMessage({
                payload: {
                    updatedData: 'msg'
                }
            })

            expect(result.processingResult).toBe('published')
        })
    })
})
