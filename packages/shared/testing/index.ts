/**
 * Shared testing utilities for Rising Intelligence services.
 *
 * Usage:
 *   import { fixtures, TestClock, MockLLM } from '@rising-intelligence/shared/testing';
 */

export { fixtures } from './fixtures';
export { TestClock } from './time';
export { MockLLM } from './mock-llm';
export { MockSourceAdapter, type MockSourceConfig } from './mock-sources';
export { setupTestInfra, teardownTestInfra, type TestContext } from './setup';
