import { AbstractSnsPublisher } from '../../lib/sns/AbstractSnsPublisher'
import type { SupportedMessages } from '../consumers/CreateLocateConfigMixConsumer'

export class CreateLocateConfigMixPublisher extends AbstractSnsPublisher<SupportedMessages> {}
