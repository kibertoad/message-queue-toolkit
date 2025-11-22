import { Storage } from '@google-cloud/storage'

export const TEST_GCS_CONFIG = {
  projectId: 'test-project',
  apiEndpoint: 'http://127.0.0.1:4443',
}

export function createTestGCSClient(): Storage {
  return new Storage({
    projectId: TEST_GCS_CONFIG.projectId,
    apiEndpoint: TEST_GCS_CONFIG.apiEndpoint,
  })
}
