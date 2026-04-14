import { describe, expect, it, vi } from 'vitest';
import type { ExecutionPlan, StartExecutionV2Result } from '../execution/types.js';
import type { ExecutionId, SessionId, UserId } from '../types/ids.js';
import {
  createPendingSessionMessageFromPlan,
  enqueuePendingSessionMessage,
  flushNextPendingSessionMessage,
} from './session-message-queue.js';
import {
  createPendingSessionMessage,
  storePendingSessionMessage,
  type PendingSessionMessage,
  type SessionQueueStorage,
} from './pending-messages.js';

type BuildPendingFlushPlan = Parameters<typeof flushNextPendingSessionMessage>[0]['buildPlan'];

function createMemoryStorage(initialEntries?: Array<[string, unknown]>): SessionQueueStorage {
  const store = new Map(initialEntries ?? []);
  return {
    async get<T = unknown>(key: string) {
      return store.get(key) as T | undefined;
    },
    async put(key, value) {
      store.set(key, value);
    },
    async delete(keys) {
      for (const key of Array.isArray(keys) ? keys : [keys]) {
        store.delete(key);
      }
    },
    async list<T = unknown>({ prefix }: { prefix: string }) {
      return new Map(
        Array.from(store.entries()).filter(([key]) => key.startsWith(prefix)) as Array<[string, T]>
      );
    },
  };
}

function createQueuedPlan() {
  return {
    executionId: 'exc_queued_retry' as ExecutionId,
    messageId: 'msg_018f1e2d3c4bAbCdEfGhIjKlMn',
    prompt: 'queued prompt',
    mode: 'plan' as const,
    wrapper: {
      kiloSessionId: 'kilo_test',
      model: { modelID: 'queued-model' },
      variant: 'beta',
      autoCommit: true,
      condenseOnComplete: true,
    },
    workspace: {
      shouldPrepare: false as const,
      sandboxId: 'sandbox_test',
      resumeContext: {
        kiloSessionId: 'kilo_test',
        workspacePath: '/tmp/workspace',
        kilocodeToken: 'token',
        kilocodeModel: 'kilo/queued-model',
        branchName: 'main',
        githubToken: 'queued-gh-token',
        gitToken: 'queued-git-token',
      },
    },
  };
}

function createContext() {
  return {
    sessionId: 'agent_test' as SessionId,
    userId: 'user_test' as UserId,
    sandboxId: 'sandbox_test',
    kiloSessionId: 'kilo_test',
    metadata: {
      initiatedAt: Date.now(),
      mode: 'code',
      model: 'default-model',
      variant: 'alpha',
      autoCommit: false,
      condenseOnComplete: false,
      kilocodeToken: 'token',
      githubToken: 'default-gh-token',
    },
  };
}

describe('session-message-queue', () => {
  it('creates queued messages with persisted execution options', () => {
    const message = createPendingSessionMessageFromPlan(createQueuedPlan(), 123);

    expect(message).toMatchObject({
      messageId: 'msg_018f1e2d3c4bAbCdEfGhIjKlMn',
      executionId: 'exc_queued_retry',
      createdAt: 123,
      executionKind: 'followup',
      executionOptions: {
        mode: 'plan',
        model: 'queued-model',
        variant: 'beta',
        autoCommit: true,
        condenseOnComplete: true,
        githubTokenOverride: 'queued-gh-token',
        gitTokenOverride: 'queued-git-token',
      },
    });
  });

  it('persists queued initiate execution intent without token overrides', () => {
    const plan = {
      ...createQueuedPlan(),
      executionKind: 'initiate' as const,
      workspace: {
        shouldPrepare: true as const,
        sandboxId: 'sandbox_test',
        initContext: {
          kilocodeToken: 'token',
          gitUrl: 'https://example.com/repo.git',
          gitToken: 'secret-git-token',
        },
      },
    };

    const message = createPendingSessionMessageFromPlan(plan, 123);

    expect(message).toMatchObject({
      executionKind: 'initiate',
      executionOptions: {
        mode: 'plan',
        model: 'queued-model',
        variant: 'beta',
        autoCommit: true,
        condenseOnComplete: true,
      },
    });
    expect(message.executionOptions?.githubTokenOverride).toBeUndefined();
    expect(message.executionOptions?.gitTokenOverride).toBeUndefined();
  });

  it('retries a queued flush after a pre-start failure without dropping the message', async () => {
    const storage = createMemoryStorage();
    await enqueuePendingSessionMessage(storage, createQueuedPlan(), 1);

    const buildPlan = vi.fn<BuildPendingFlushPlan>(
      async ({ message, executionId, options, context }) => {
        const plan = {
          executionId,
          sessionId: context.sessionId,
          userId: context.userId,
          orgId: context.orgId,
          prompt: message.content,
          mode: options.mode ?? 'code',
          workspace: {
            shouldPrepare: false,
            sandboxId: context.sandboxId,
            resumeContext: {
              kiloSessionId: context.kiloSessionId ?? '',
              workspacePath: '/tmp/workspace',
              kilocodeToken: context.metadata.kilocodeToken ?? '',
              kilocodeModel: options.model,
              branchName: 'main',
              githubToken: options.githubTokenOverride ?? context.metadata.githubToken,
              gitToken: options.gitTokenOverride,
            },
          },
          wrapper: {
            kiloSessionId: context.kiloSessionId,
            model: options.model ? { modelID: options.model } : undefined,
            variant: options.variant,
            autoCommit: options.autoCommit,
            condenseOnComplete: options.condenseOnComplete,
          },
          messageId: message.messageId,
        } satisfies ExecutionPlan;
        return plan;
      }
    );

    const deliver = vi
      .fn<(_: ExecutionPlan, __: PendingSessionMessage) => Promise<StartExecutionV2Result>>()
      .mockResolvedValueOnce({
        success: false,
        code: 'INTERNAL',
        error: 'workspace restore failed',
      })
      .mockResolvedValueOnce({
        success: true,
        executionId: 'exc_queued_retry' as ExecutionId,
        status: 'started',
        messageId: 'msg_018f1e2d3c4bAbCdEfGhIjKlMn',
        delivery: 'sent',
      });

    const first = await flushNextPendingSessionMessage({
      storage,
      now: 10,
      hasCurrentRuntimeExecution: async () => false,
      getMetadataContext: async () => createContext(),
      buildPlan,
      deliver,
    });

    expect(first.type).toBe('failure');
    if (first.type !== 'failure') return;
    expect(first.message.flushAttempts).toBe(1);
    expect(first.message.executionKind).toBe('followup');

    const second = await flushNextPendingSessionMessage({
      storage,
      now: first.nextFlushAttemptAt ?? 20,
      hasCurrentRuntimeExecution: async () => false,
      getMetadataContext: async () => createContext(),
      buildPlan,
      deliver,
    });

    expect(second).toEqual({ type: 'delivered' });
    expect(deliver).toHaveBeenCalledTimes(2);
    const secondBuildPlanCall = buildPlan.mock.calls[1];
    expect(secondBuildPlanCall).toBeDefined();
    if (!secondBuildPlanCall) return;
    const [secondBuildPlanArgs] = secondBuildPlanCall;
    expect(secondBuildPlanArgs.executionId).toBe('exc_queued_retry');
    expect(secondBuildPlanArgs.executionKind).toBe('followup');
    expect(secondBuildPlanArgs.options).toMatchObject({
      mode: 'plan',
      model: 'queued-model',
      githubTokenOverride: 'queued-gh-token',
      gitTokenOverride: 'queued-git-token',
    });
    expect((await storage.list({ prefix: 'pending_message:' })).size).toBe(0);
  });

  it('infers initiatePrepared for legacy pending records matching initialMessageId', async () => {
    const storage = createMemoryStorage();
    await storePendingSessionMessage(
      storage,
      createPendingSessionMessage({
        messageId: 'msg_018f1e2d3c4bLegacyInitAbCD',
        executionId: 'exc_legacy_initial',
        role: 'user',
        content: 'initial prompt',
        createdAt: 1,
      })
    );

    const buildPlan = vi.fn<BuildPendingFlushPlan>(
      async ({ message, executionId, executionKind, context }) => {
        return {
          executionId,
          sessionId: context.sessionId,
          userId: context.userId,
          prompt: message.content,
          mode: 'code',
          workspace: {
            shouldPrepare: true,
            sandboxId: context.sandboxId,
            initContext: {
              kilocodeToken: context.metadata.kilocodeToken ?? '',
              isPreparedSession: executionKind === 'initiatePrepared',
            },
          },
          wrapper: {},
          messageId: message.messageId,
        } satisfies ExecutionPlan;
      }
    );

    const delivered: ExecutionPlan[] = [];
    const result = await flushNextPendingSessionMessage({
      storage,
      now: 10,
      hasCurrentRuntimeExecution: async () => false,
      getMetadataContext: async () => ({
        ...createContext(),
        metadata: {
          ...createContext().metadata,
          initialMessageId: 'msg_018f1e2d3c4bLegacyInitAbCD',
          prompt: 'initial prompt',
        },
      }),
      buildPlan,
      deliver: async plan => {
        delivered.push(plan);
        return {
          success: true,
          executionId: plan.executionId,
          status: 'started',
          messageId: plan.messageId,
          delivery: 'sent',
        };
      },
    });

    expect(result).toEqual({ type: 'delivered' });
    expect(buildPlan).toHaveBeenCalledWith(
      expect.objectContaining({ executionKind: 'initiatePrepared' })
    );
    expect(delivered[0]?.workspace.shouldPrepare).toBe(true);
  });
});
