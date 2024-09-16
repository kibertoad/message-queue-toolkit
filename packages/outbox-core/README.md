# outbox-core

Main package that contains the core functionality of the Outbox pattern to provide "at least once" delivery semantics for messages.

## Installation

```bash
npm i -S @message-queue-toolkit/outbox-core
```

## Usage

To process outbox entries and emit them to the message queue, you need to create an instance of the `OutboxPeriodicJob` class:

```typescript
import { OutboxPeriodicJob } from '@message-queue-toolkit/outbox-core';

const job = new OutboxPeriodicJob(
    //Implementation of OutboxStorage interface, TODO: Point to other packages in message-queue-toolkit
    outboxStorage, 
    //Default available accumulator for gathering outbox entries as the process job is progressing.
    new InMemoryOutboxAccumulator(),
    //DomainEventEmitter, it will be used to publish events, see @message-queue-toolkit/core
    eventEmitter,
    //See PeriodicJobDependencies from @lokalise/background-jobs-common
    dependencies,
    //Retry count, how many times outbox entries should be retried to be processed
    3,
    //emitBatchSize - how many outbox entries should be emitted at once
    10,
    //internalInMs - how often the job should be executed, e.g. below it runs every 1sec
    1000
)
```

Job will take care of processing outbox entries emitted by:
```typescript
import {
  type CommonEventDefinition,
  enrichMessageSchemaWithBase,
} from '@message-queue-toolkit/schemas'

const MyEvents = {
  created: {
    ...enrichMessageSchemaWithBase(
      'entity.created',
      z.object({
        message: z.string(),
      }),
    ),
  },
} as const satisfies Record<string, CommonEventDefinition>

type MySupportedEvents = (typeof TestEvents)[keyof typeof TestEvents][]

const emitter = new OutboxEventEmitter<MySupportedEvents>(
    //Same instance of outbox storage that is used by OutboxPeriodicJob
    outboxStorage
)

//It pushes the entry to the storage, later will be picked up by the OutboxPeriodicJob
await emitter.emit(/* args */)
```
