import {
  PERMISSIONS_ADD_MESSAGE_SCHEMA,
  PERMISSIONS_REMOVE_MESSAGE_SCHEMA,
} from '../../test/consumers/userConsumerSchemas'

import { generateFilterAttributes } from './snsAttributeUtils'

describe('snsAttributeUtils', () => {
  it('resolves filter for a single schema', () => {
    const resolvedFilter = generateFilterAttributes([PERMISSIONS_ADD_MESSAGE_SCHEMA], 'messageType')

    expect(resolvedFilter).toEqual({
      FilterPolicy: `{"type":["add"]}`,
      FilterPolicyScope: 'MessageBody',
    })
  })

  it('resolves filter for multiple schemas', () => {
    const resolvedFilter = generateFilterAttributes(
      [PERMISSIONS_REMOVE_MESSAGE_SCHEMA, PERMISSIONS_ADD_MESSAGE_SCHEMA],
      'messageType',
    )

    expect(resolvedFilter).toEqual({
      FilterPolicy: `{"type":["remove","add"]}`,
      FilterPolicyScope: 'MessageBody',
    })
  })
})
