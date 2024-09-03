export interface OutboxStorage {
    saveMessage(message: unknown): Promise<void>
    getMessage(): Promise<unknown>
    deleteMessage(): Promise<void>
}

export class OutboxProcessor {

}
