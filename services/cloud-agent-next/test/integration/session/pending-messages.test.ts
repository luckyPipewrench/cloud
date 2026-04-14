import { env, runInDurableObject } from 'cloudflare:test';
import { drizzle } from 'drizzle-orm/durable-sqlite';
import { describe, expect, it } from 'vitest';
import {
  PENDING_SESSION_MESSAGE_LIMIT,
  clearPendingSessionMessages,
  countPendingSessionMessages,
  deletePendingSessionMessageByMessageId,
  findPendingSessionMessageByClientRequestId,
  listPendingSessionMessages,
  storePendingSessionMessage,
  type PendingSessionMessage,
} from '../../../src/session/pending-messages.js';
import { createEventQueries } from '../../../src/session/queries/events.js';

const createMessage = (overrides: Partial<PendingSessionMessage>): PendingSessionMessage => ({
  messageId: 'msg_018f1e2d3c4bAbCdEfGhIjKlMn',
  role: 'user',
  content: 'hello',
  createdAt: 1,
  ...overrides,
});

describe('pending session messages', () => {
  it('lists messages in FIFO key order', async () => {
    const userId = 'user_pending_fifo';
    const sessionId = 'agent_pending_fifo';
    const stub = env.CLOUD_AGENT_SESSION.get(
      env.CLOUD_AGENT_SESSION.idFromName(`${userId}:${sessionId}`)
    );

    const messages = await runInDurableObject(stub, async instance => {
      await storePendingSessionMessage(
        instance.ctx.storage,
        createMessage({ messageId: 'msg_018f1e2d3c4bBBBBBBBBBBBBBB', createdAt: 20 })
      );
      await storePendingSessionMessage(
        instance.ctx.storage,
        createMessage({ messageId: 'msg_018f1e2d3c4bAAAAAAAAAAAAAA', createdAt: 10 })
      );

      return listPendingSessionMessages(instance.ctx.storage);
    });

    expect(messages.map(message => message.messageId)).toEqual([
      'msg_018f1e2d3c4bAAAAAAAAAAAAAA',
      'msg_018f1e2d3c4bBBBBBBBBBBBBBB',
    ]);
  });

  it('deletes every matching messageId', async () => {
    const userId = 'user_pending_delete';
    const sessionId = 'agent_pending_delete';
    const stub = env.CLOUD_AGENT_SESSION.get(
      env.CLOUD_AGENT_SESSION.idFromName(`${userId}:${sessionId}`)
    );

    const result = await runInDurableObject(stub, async instance => {
      await storePendingSessionMessage(
        instance.ctx.storage,
        createMessage({ messageId: 'msg_018f1e2d3c4bDelMsgAbCdEfGh', createdAt: 1 })
      );
      await storePendingSessionMessage(
        instance.ctx.storage,
        createMessage({ messageId: 'msg_018f1e2d3c4bDelMsgAbCdEfGh', createdAt: 2 })
      );
      await storePendingSessionMessage(
        instance.ctx.storage,
        createMessage({ messageId: 'msg_018f1e2d3c4bKeepMsgAbCdEfG', createdAt: 3 })
      );

      const deleted = await deletePendingSessionMessageByMessageId(
        instance.ctx.storage,
        'msg_018f1e2d3c4bDelMsgAbCdEfGh'
      );
      const missing = await deletePendingSessionMessageByMessageId(
        instance.ctx.storage,
        'msg_018f1e2d3c4bMissingMessage'
      );
      const remaining = await listPendingSessionMessages(instance.ctx.storage);
      return { deleted, missing, remaining };
    });

    expect(result.deleted).toBe(true);
    expect(result.missing).toBe(false);
    expect(result.remaining.map(message => message.messageId)).toEqual([
      'msg_018f1e2d3c4bKeepMsgAbCdEfG',
    ]);
  });

  it('finds by clientRequestId', async () => {
    const userId = 'user_pending_client_request';
    const sessionId = 'agent_pending_client_request';
    const stub = env.CLOUD_AGENT_SESSION.get(
      env.CLOUD_AGENT_SESSION.idFromName(`${userId}:${sessionId}`)
    );

    const found = await runInDurableObject(stub, async instance => {
      await storePendingSessionMessage(
        instance.ctx.storage,
        createMessage({
          messageId: 'msg_018f1e2d3c4bCliReqAbCdEfGh',
          executionId: 'exc_compatibility',
          clientRequestId: 'client-request-1',
        })
      );

      return findPendingSessionMessageByClientRequestId(instance.ctx.storage, 'client-request-1');
    });

    expect(found?.messageId).toBe('msg_018f1e2d3c4bCliReqAbCdEfGh');
    expect(found?.executionId).toBe('exc_compatibility');
  });

  it('ignores invalid stored entries', async () => {
    const userId = 'user_pending_invalid';
    const sessionId = 'agent_pending_invalid';
    const stub = env.CLOUD_AGENT_SESSION.get(
      env.CLOUD_AGENT_SESSION.idFromName(`${userId}:${sessionId}`)
    );

    const messages = await runInDurableObject(stub, async instance => {
      await instance.ctx.storage.put('pending_message:0000000000000001:invalid', {
        messageId: 'msg_018F1e2d3c4bAbCdEfGhIjKlMn',
        role: 'user',
        content: 'bad',
        createdAt: 1,
      });
      await storePendingSessionMessage(
        instance.ctx.storage,
        createMessage({ messageId: 'msg_018f1e2d3c4bValidMsgAbCdEf', createdAt: 2 })
      );

      return listPendingSessionMessages(instance.ctx.storage);
    });

    expect(messages.map(message => message.messageId)).toEqual(['msg_018f1e2d3c4bValidMsgAbCdEf']);
  });

  it('clears valid messages and ignores invalid stored entries', async () => {
    const userId = 'user_pending_clear';
    const sessionId = 'agent_pending_clear';
    const stub = env.CLOUD_AGENT_SESSION.get(
      env.CLOUD_AGENT_SESSION.idFromName(`${userId}:${sessionId}`)
    );

    const result = await runInDurableObject(stub, async instance => {
      await instance.ctx.storage.put('pending_message:0000000000000001:invalid', {
        messageId: 'invalid',
        role: 'user',
        content: 'bad',
        createdAt: 1,
      });
      await storePendingSessionMessage(
        instance.ctx.storage,
        createMessage({ messageId: 'msg_018f1e2d3c4bClearAMsgAbCdE', createdAt: 2 })
      );
      await storePendingSessionMessage(
        instance.ctx.storage,
        createMessage({ messageId: 'msg_018f1e2d3c4bClearBMsgAbCdE', createdAt: 3 })
      );

      const cleared = await clearPendingSessionMessages(instance.ctx.storage);
      const remaining = await listPendingSessionMessages(instance.ctx.storage);
      const rawInvalid = await instance.ctx.storage.get('pending_message:0000000000000001:invalid');
      return { cleared, remaining, rawInvalid };
    });

    expect(result.cleared.map(message => message.messageId)).toEqual([
      'msg_018f1e2d3c4bClearAMsgAbCdE',
      'msg_018f1e2d3c4bClearBMsgAbCdE',
    ]);
    expect(result.remaining).toHaveLength(0);
    expect(result.rawInvalid).toBeDefined();
  });

  it('counts messages up to the queue limit', async () => {
    const userId = 'user_pending_count';
    const sessionId = 'agent_pending_count';
    const stub = env.CLOUD_AGENT_SESSION.get(
      env.CLOUD_AGENT_SESSION.idFromName(`${userId}:${sessionId}`)
    );

    const count = await runInDurableObject(stub, async instance => {
      for (let index = 0; index < PENDING_SESSION_MESSAGE_LIMIT; index++) {
        await storePendingSessionMessage(
          instance.ctx.storage,
          createMessage({
            messageId: `msg_018f1e2d3c4b${String(index).padStart(14, 'A')}`,
            createdAt: index,
          })
        );
      }

      return countPendingSessionMessages(instance.ctx.storage);
    });

    expect(count).toBe(PENDING_SESSION_MESSAGE_LIMIT);
  });

  it('flushes one FIFO message on alarm and deletes after orchestrator accepts', async () => {
    const userId = 'user_pending_flush';
    const sessionId = 'agent_pending_flush';
    const stub = env.CLOUD_AGENT_SESSION.get(
      env.CLOUD_AGENT_SESSION.idFromName(`${userId}:${sessionId}`)
    );

    const result = await runInDurableObject(stub, async instance => {
      let acceptedMessageId: string | undefined;
      (instance as any).orchestrator = {
        execute: async (plan: any) => {
          acceptedMessageId = plan.messageId;
          return { messageId: plan.messageId, kiloSessionId: 'kilo_test' };
        },
      };
      await instance.prepare({
        sessionId,
        userId,
        kiloSessionId: '55555555-5555-4555-5555-555555555555',
        prompt: 'prepared prompt',
        mode: 'code',
        model: 'test-model',
        kilocodeToken: 'token-followup',
        gitUrl: 'https://example.com/repo.git',
        gitToken: 'old-token',
      });
      await instance.tryInitiate();
      await storePendingSessionMessage(
        instance.ctx.storage,
        createMessage({
          messageId: 'msg_018f1e2d3c4bFlushMsgAbCdEf',
          executionId: 'exc_flush',
          content: 'flush me',
          createdAt: 1,
        })
      );

      await instance.alarm();
      const pending = await listPendingSessionMessages(instance.ctx.storage);
      const currentRuntimeExecution = await instance.getCurrentRuntimeExecution();
      return { acceptedMessageId, pending, currentRuntimeExecution };
    });

    expect(result.acceptedMessageId).toBe('msg_018f1e2d3c4bFlushMsgAbCdEf');
    expect(result.pending).toHaveLength(0);
    expect(result.currentRuntimeExecution?.executionId).toBe('exc_flush');
  });

  it('keeps queued messages when flush returns an unsuccessful result without throwing', async () => {
    const userId = 'user_pending_flush_unsuccessful';
    const sessionId = 'agent_pending_flush_unsuccessful';
    const stub = env.CLOUD_AGENT_SESSION.get(
      env.CLOUD_AGENT_SESSION.idFromName(`${userId}:${sessionId}`)
    );

    const result = await runInDurableObject(stub, async instance => {
      instance['executeDirectly'] = async () => ({
        success: false,
        code: 'INTERNAL',
        error: 'execution add failed',
      });

      await instance.prepare({
        sessionId,
        userId,
        kiloSessionId: '56565656-5656-4565-8565-565656565656',
        prompt: 'prepared prompt',
        mode: 'code',
        model: 'test-model',
        kilocodeToken: 'token-followup',
        gitUrl: 'https://example.com/repo.git',
        gitToken: 'old-token',
      });
      await instance.tryInitiate();
      await storePendingSessionMessage(
        instance.ctx.storage,
        createMessage({
          messageId: 'msg_018f1e2d3c4bFlushResAbCdEf',
          executionId: 'exc_flush_result_fail',
          content: 'flush me later',
          createdAt: 1,
        })
      );

      await instance.alarm();
      const pending = await listPendingSessionMessages(instance.ctx.storage);
      const alarm = await instance.ctx.storage.getAlarm();
      return { pending, alarm };
    });

    expect(result.pending.map(message => message.messageId)).toEqual([
      'msg_018f1e2d3c4bFlushResAbCdEf',
    ]);
    expect(result.pending[0]?.flushAttempts).toBe(1);
    expect(result.pending[0]?.lastFlushError).toBe('execution add failed');
    expect(result.pending[0]?.nextFlushAttemptAt).toBeGreaterThan(Date.now());
    expect(result.alarm).toBe(result.pending[0]?.nextFlushAttemptAt);
  });

  it('records a failed flush attempt and schedules a delayed retry', async () => {
    const userId = 'user_pending_flush_fail';
    const sessionId = 'agent_pending_flush_fail';
    const stub = env.CLOUD_AGENT_SESSION.get(
      env.CLOUD_AGENT_SESSION.idFromName(`${userId}:${sessionId}`)
    );

    const result = await runInDurableObject(stub, async instance => {
      (instance as any).orchestrator = {
        execute: async () => {
          throw new Error('wrapper unavailable');
        },
      };
      await instance.prepare({
        sessionId,
        userId,
        kiloSessionId: '44444444-4444-4444-4444-444444444444',
        prompt: 'prepared prompt',
        mode: 'code',
        model: 'test-model',
        kilocodeToken: 'token-followup',
        gitUrl: 'https://example.com/repo.git',
        gitToken: 'old-token',
      });
      await instance.tryInitiate();
      await storePendingSessionMessage(
        instance.ctx.storage,
        createMessage({
          messageId: 'msg_018f1e2d3c4bFlushFalAbCdEf',
          executionId: 'exc_flush_fail',
          content: 'flush me later',
          createdAt: 1,
        })
      );

      await instance.alarm();
      const pending = await listPendingSessionMessages(instance.ctx.storage);
      const alarm = await instance.ctx.storage.getAlarm();
      return { pending, alarm };
    });

    expect(result.pending.map(message => message.messageId)).toEqual([
      'msg_018f1e2d3c4bFlushFalAbCdEf',
    ]);
    expect(result.pending[0]?.flushAttempts).toBe(1);
    expect(result.pending[0]?.lastFlushError).toBe('wrapper unavailable');
    expect(result.pending[0]?.nextFlushAttemptAt).toBeGreaterThan(Date.now());
    expect(result.alarm).toBe(result.pending[0]?.nextFlushAttemptAt);
  });

  it('schedules wrapper liveness before a delayed pending retry without one-second churn', async () => {
    const userId = 'user_pending_retry_liveness';
    const sessionId = 'agent_pending_retry_liveness';
    const stub = env.CLOUD_AGENT_SESSION.get(
      env.CLOUD_AGENT_SESSION.idFromName(`${userId}:${sessionId}`)
    );

    const result = await runInDurableObject(stub, async instance => {
      const now = Date.now();
      const livenessDeadline = now + 5_000;
      const pendingRetryAt = now + 15_000;
      await instance.prepare({
        sessionId,
        userId,
        kiloSessionId: '57575757-5757-4575-8575-575757575757',
        prompt: 'prepared prompt',
        mode: 'code',
        model: 'test-model',
        kilocodeToken: 'token-followup',
        gitUrl: 'https://example.com/repo.git',
        gitToken: 'old-token',
      });
      await instance.tryInitiate();
      await instance.addExecution({
        executionId: 'exc_liveness_before_pending',
        mode: 'code',
        streamingMode: 'websocket',
        ingestToken: 'exc_liveness_before_pending',
        messageId: 'msg_018f1e2d3c4bLiveBeforePend',
      });
      await instance.ctx.storage.put('wrapper_runtime_state', {
        wrapperGeneration: 1,
        wrapperConnectionId: 'conn_liveness_before_pending',
        wrapperExecutionId: 'exc_liveness_before_pending',
        acceptedExecutionId: 'exc_liveness_before_pending',
        nextPingAt: livenessDeadline,
      });
      await storePendingSessionMessage(
        instance.ctx.storage,
        createMessage({
          messageId: 'msg_018f1e2d3c4bDelayRetryPend',
          executionId: 'exc_delay_retry_pending',
          content: 'retry later',
          createdAt: 1,
          nextFlushAttemptAt: pendingRetryAt,
        })
      );

      await instance.alarm();
      const alarm = await instance.ctx.storage.getAlarm();
      return { alarm, livenessDeadline, pendingRetryAt, now };
    });

    expect(result.alarm).toBeGreaterThanOrEqual(result.livenessDeadline);
    expect(result.alarm).toBeLessThan(result.pendingRetryAt);
    expect(result.alarm).toBeGreaterThan(result.now + 1_000);
  });

  it('exhausts failed flush retries, emits message.failed, and removes the pending message', async () => {
    const userId = 'user_pending_flush_exhaust';
    const sessionId = 'agent_pending_flush_exhaust';
    const stub = env.CLOUD_AGENT_SESSION.get(
      env.CLOUD_AGENT_SESSION.idFromName(`${userId}:${sessionId}`)
    );

    const result = await runInDurableObject(stub, async (instance, state) => {
      (instance as any).orchestrator = {
        execute: async () => {
          throw new Error('wrapper still unavailable');
        },
      };
      await instance.prepare({
        sessionId,
        userId,
        kiloSessionId: '45454545-4545-4545-8545-454545454545',
        prompt: 'prepared prompt',
        mode: 'code',
        model: 'test-model',
        kilocodeToken: 'token-followup',
      });
      await instance.tryInitiate();
      await storePendingSessionMessage(
        instance.ctx.storage,
        createMessage({
          messageId: 'msg_018f1e2d3c4bAAAAAAAAAAAAAA',
          executionId: 'exc_flush_exhaust',
          content: 'flush until exhausted',
          createdAt: 1,
          flushAttempts: 4,
          nextFlushAttemptAt: Date.now() - 1,
        })
      );

      await instance.alarm();
      const pending = await listPendingSessionMessages(instance.ctx.storage);
      const db = drizzle(state.storage, { logger: false });
      const eventQueries = createEventQueries(db, state.storage.sql);
      const events = eventQueries.findByFilters({ executionIds: ['exc_flush_exhaust'] });
      return {
        pending,
        events: events.map(event => ({ ...event, payload: JSON.parse(event.payload) })),
      };
    });

    expect(result.pending).toHaveLength(0);
    const failedEvent = result.events.find(
      event => event.stream_event_type === 'message.failed' && event.payload.accepted !== true
    );
    expect(failedEvent).toBeDefined();
    const payload = failedEvent?.payload ?? {};
    expect(payload).toMatchObject({
      messageId: 'msg_018f1e2d3c4bAAAAAAAAAAAAAA',
      executionId: 'exc_flush_exhaust',
      error: 'wrapper still unavailable',
      attempts: 5,
    });
  });

  it('interrupt clears pending messages and emits message.failed for each queued message', async () => {
    const userId = 'user_pending_interrupt_clear';
    const sessionId = 'agent_pending_interrupt_clear';
    const stub = env.CLOUD_AGENT_SESSION.get(
      env.CLOUD_AGENT_SESSION.idFromName(`${userId}:${sessionId}`)
    );

    const result = await runInDurableObject(stub, async (instance, state) => {
      await instance.prepare({
        sessionId,
        userId,
        kiloSessionId: '66666666-6666-4666-8666-666666666666',
        prompt: 'prepared prompt',
        mode: 'code',
        model: 'test-model',
        kilocodeToken: 'token-followup',
      });
      await storePendingSessionMessage(
        instance.ctx.storage,
        createMessage({
          messageId: 'msg_018f1e2d3c4bIntrAMsgAbCdEf',
          executionId: 'exc_interrupt_a',
          content: 'first queued',
          createdAt: 1,
        })
      );
      await storePendingSessionMessage(
        instance.ctx.storage,
        createMessage({
          messageId: 'msg_018f1e2d3c4bIntrBMsgAbCdEf',
          executionId: 'exc_interrupt_b',
          content: 'second queued',
          createdAt: 2,
        })
      );

      const interrupt = await instance.interruptExecution();
      const pending = await listPendingSessionMessages(instance.ctx.storage);
      const db = drizzle(state.storage, { logger: false });
      const eventQueries = createEventQueries(db, state.storage.sql);
      const events = eventQueries.findByFilters({
        executionIds: ['exc_interrupt_a', 'exc_interrupt_b'],
      });
      return { interrupt, pending, events };
    });

    expect(result.interrupt.success).toBe(true);
    expect(result.pending).toHaveLength(0);
    const failedPayloads = result.events
      .filter(event => event.stream_event_type === 'message.failed')
      .map(event => JSON.parse(event.payload));
    expect(failedPayloads).toEqual([
      {
        messageId: 'msg_018f1e2d3c4bIntrAMsgAbCdEf',
        executionId: 'exc_interrupt_a',
        error: 'Pending queued message interrupted by user',
        reason: 'interrupted',
        delivery: 'queued',
      },
      {
        messageId: 'msg_018f1e2d3c4bIntrBMsgAbCdEf',
        executionId: 'exc_interrupt_b',
        error: 'Pending queued message interrupted by user',
        reason: 'interrupted',
        delivery: 'queued',
      },
    ]);
  });

  it('interrupt with pending-only and no current runtime execution returns success', async () => {
    const userId = 'user_pending_interrupt_only';
    const sessionId = 'agent_pending_interrupt_only';
    const stub = env.CLOUD_AGENT_SESSION.get(
      env.CLOUD_AGENT_SESSION.idFromName(`${userId}:${sessionId}`)
    );

    const result = await runInDurableObject(stub, async instance => {
      await instance.prepare({
        sessionId,
        userId,
        kiloSessionId: '77777777-7777-4777-8777-777777777777',
        prompt: 'prepared prompt',
        mode: 'code',
        model: 'test-model',
        kilocodeToken: 'token-followup',
      });
      await storePendingSessionMessage(
        instance.ctx.storage,
        createMessage({
          messageId: 'msg_018f1e2d3c4bPendOnlyAbCdEf',
          executionId: 'exc_pending_only',
          content: 'queued only',
          createdAt: 1,
        })
      );

      const interrupt = await instance.interruptExecution();
      const pending = await listPendingSessionMessages(instance.ctx.storage);
      const currentRuntimeExecution = await instance.getCurrentRuntimeExecution();
      return { interrupt, pending, currentRuntimeExecution };
    });

    expect(result.interrupt).toEqual({ success: true, executionId: undefined });
    expect(result.pending).toHaveLength(0);
    expect(result.currentRuntimeExecution).toBeNull();
  });

  it('interrupt with current wrapper runtime execution sends kill and clears queued messages', async () => {
    const userId = 'user_pending_interrupt_active';
    const sessionId = 'agent_pending_interrupt_active';
    const stub = env.CLOUD_AGENT_SESSION.get(
      env.CLOUD_AGENT_SESSION.idFromName(`${userId}:${sessionId}`)
    );

    const result = await runInDurableObject(stub, async instance => {
      const sentCommands: unknown[] = [];
      instance.sendToWrapper = (_executionId, command) => {
        sentCommands.push(command);
      };
      await instance.prepare({
        sessionId,
        userId,
        kiloSessionId: '88888888-8888-4888-8888-888888888888',
        prompt: 'prepared prompt',
        mode: 'code',
        model: 'test-model',
        kilocodeToken: 'token-followup',
      });
      await instance.addExecution({
        executionId: 'exc_interrupt_active',
        mode: 'code',
        streamingMode: 'websocket',
        ingestToken: 'exc_interrupt_active',
        messageId: 'msg_018f1e2d3c4bAcceptActAbCdE',
      });
      await instance.ctx.storage.put('wrapper_runtime_state', {
        wrapperGeneration: 1,
        wrapperConnectionId: 'conn_interrupt_active',
        wrapperExecutionId: 'exc_interrupt_active',
        acceptedMessageId: 'msg_018f1e2d3c4bAcceptActAbCdE',
        acceptedExecutionId: 'exc_interrupt_active',
      });
      await storePendingSessionMessage(
        instance.ctx.storage,
        createMessage({
          messageId: 'msg_018f1e2d3c4bActQueueAbCdEf',
          executionId: 'exc_interrupt_queued',
          content: 'queued behind active',
          createdAt: 1,
        })
      );

      const interrupt = await instance.interruptExecution();
      const pending = await listPendingSessionMessages(instance.ctx.storage);
      const currentRuntimeExecution = await instance.getCurrentRuntimeExecution();
      return { interrupt, pending, currentRuntimeExecution, sentCommands };
    });

    expect(result.interrupt.success).toBe(true);
    expect(result.interrupt.executionId).toBe('exc_interrupt_active');
    expect(result.sentCommands).toEqual([{ type: 'kill', signal: 'SIGTERM' }]);
    expect(result.pending).toHaveLength(0);
    expect(result.currentRuntimeExecution?.messageId).toBe('msg_018f1e2d3c4bAcceptActAbCdE');
  });

  it('defers pending messages on debounce cadence while current runtime execution exists', async () => {
    const userId = 'user_pending_flush_active';
    const sessionId = 'agent_pending_flush_active';
    const stub = env.CLOUD_AGENT_SESSION.get(
      env.CLOUD_AGENT_SESSION.idFromName(`${userId}:${sessionId}`)
    );

    const result = await runInDurableObject(stub, async instance => {
      let callCount = 0;
      (instance as any).orchestrator = {
        execute: async (plan: any) => {
          callCount += 1;
          return { messageId: plan.messageId, kiloSessionId: 'kilo_test' };
        },
      };
      await instance.prepare({
        sessionId,
        userId,
        kiloSessionId: '33333333-3333-4333-3333-333333333333',
        prompt: 'prepared prompt',
        mode: 'code',
        model: 'test-model',
        kilocodeToken: 'token-followup',
        gitUrl: 'https://example.com/repo.git',
        gitToken: 'old-token',
      });
      await instance.tryInitiate();
      await instance.addExecution({
        executionId: 'exc_active_flush',
        mode: 'code',
        streamingMode: 'websocket',
        ingestToken: 'exc_active_flush',
      });
      await instance.ctx.storage.put('wrapper_runtime_state', {
        wrapperGeneration: 1,
        wrapperConnectionId: 'conn_active_flush',
        wrapperExecutionId: 'exc_active_flush',
        acceptedExecutionId: 'exc_active_flush',
      });
      await storePendingSessionMessage(
        instance.ctx.storage,
        createMessage({
          messageId: 'msg_018f1e2d3c4bActFlushOneAbC',
          executionId: 'exc_active_pending_one',
          content: 'first',
          createdAt: 1,
        })
      );
      await storePendingSessionMessage(
        instance.ctx.storage,
        createMessage({
          messageId: 'msg_018f1e2d3c4bActFlushTwoAbC',
          executionId: 'exc_active_pending_two',
          content: 'second',
          createdAt: 2,
        })
      );

      const beforeAlarm = Date.now();
      await instance.alarm();
      const pending = await listPendingSessionMessages(instance.ctx.storage);
      const alarm = await instance.ctx.storage.getAlarm();
      return { callCount, pending, alarm, beforeAlarm };
    });

    expect(result.callCount).toBe(0);
    expect(result.pending.map(message => message.messageId)).toEqual([
      'msg_018f1e2d3c4bActFlushOneAbC',
      'msg_018f1e2d3c4bActFlushTwoAbC',
    ]);
    expect(result.alarm).toBeGreaterThanOrEqual(result.beforeAlarm + 1_000);
    expect(result.alarm).toBeLessThan(result.beforeAlarm + 10_000);
  });

  it('metadata-not-ready flush keeps pending and schedules retry', async () => {
    const userId = 'user_pending_not_ready';
    const sessionId = 'agent_pending_not_ready';
    const stub = env.CLOUD_AGENT_SESSION.get(
      env.CLOUD_AGENT_SESSION.idFromName(`${userId}:${sessionId}`)
    );

    const result = await runInDurableObject(stub, async instance => {
      await instance.updateMetadata({
        version: Date.now(),
        sessionId,
        userId,
        timestamp: Date.now(),
        mode: 'code',
        model: 'test-model',
      });
      await storePendingSessionMessage(
        instance.ctx.storage,
        createMessage({
          messageId: 'msg_018f1e2d3c4bNotReadyAbCdEf',
          executionId: 'exc_not_ready_flush',
          content: 'wait for initiation',
          createdAt: 1,
        })
      );

      await instance.alarm();
      const pending = await listPendingSessionMessages(instance.ctx.storage);
      const alarm = await instance.ctx.storage.getAlarm();
      return { pending, alarm };
    });

    expect(result.pending.map(message => message.messageId)).toEqual([
      'msg_018f1e2d3c4bNotReadyAbCdEf',
    ]);
    expect(result.alarm).toBeGreaterThan(Date.now());
  });

  it('accepted execution completion emits message.completed with messageId and executionId', async () => {
    const userId = 'user_accepted_completed';
    const sessionId = 'agent_accepted_completed';
    const stub = env.CLOUD_AGENT_SESSION.get(
      env.CLOUD_AGENT_SESSION.idFromName(`${userId}:${sessionId}`)
    );

    const result = await runInDurableObject(stub, async instance => {
      await instance.prepare({
        sessionId,
        userId,
        prompt: 'prepared prompt',
        mode: 'code',
        model: 'test-model',
        kilocodeToken: 'token-completed',
      });
      await instance.addExecution({
        executionId: 'exc_accepted_completed',
        mode: 'code',
        streamingMode: 'websocket',
        ingestToken: 'exc_accepted_completed',
        messageId: 'msg_018f1e2d3c4bAcceptDoneAbCd',
      });

      await instance.updateExecutionStatus({
        executionId: 'exc_accepted_completed',
        status: 'running',
      });
      await instance.updateExecutionStatus({
        executionId: 'exc_accepted_completed',
        status: 'completed',
        gateResult: 'pass',
      });
      const duplicateResult = await instance.updateExecutionStatus({
        executionId: 'exc_accepted_completed',
        status: 'completed',
      });

      const eventQueries = createEventQueries(
        drizzle(instance.ctx.storage, { logger: false }),
        instance.ctx.storage.sql
      );
      const events = eventQueries.findByFilters({ eventTypes: ['message.completed'] });
      return { events, duplicateResult };
    });

    expect(result.duplicateResult.ok).toBe(true);
    expect(result.events).toHaveLength(1);
    expect(JSON.parse(result.events[0].payload)).toEqual({
      messageId: 'msg_018f1e2d3c4bAcceptDoneAbCd',
      executionId: 'exc_accepted_completed',
      status: 'completed',
      gateResult: 'pass',
      delivery: 'sent',
      accepted: true,
    });
  });

  it('suppressed accepted execution completion skips terminal event and callback enqueue', async () => {
    const userId = 'user_accepted_suppressed';
    const sessionId = 'agent_accepted_suppressed';
    const stub = env.CLOUD_AGENT_SESSION.get(
      env.CLOUD_AGENT_SESSION.idFromName(`${userId}:${sessionId}`)
    );

    const result = await runInDurableObject(stub, async instance => {
      const sentCallbackJobs: unknown[] = [];
      (
        instance.env as typeof instance.env & {
          CALLBACK_QUEUE: { send: (job: unknown) => Promise<void> };
        }
      ).CALLBACK_QUEUE = {
        send: async job => {
          sentCallbackJobs.push(job);
        },
      };

      await instance.prepare({
        sessionId,
        userId,
        prompt: 'prepared prompt',
        mode: 'code',
        model: 'test-model',
        kiloSessionId: '33333333-3333-4333-3333-333333333333',
        kilocodeToken: 'token-suppressed',
        callbackTarget: { url: 'https://example.com/callback' },
      });
      await instance.addExecution({
        executionId: 'exc_accepted_suppressed',
        mode: 'code',
        streamingMode: 'websocket',
        ingestToken: 'exc_accepted_suppressed',
        messageId: 'msg_018f1e2d3c4bAcceptMuteAbCd',
      });

      await instance.updateExecutionStatus(
        {
          executionId: 'exc_accepted_suppressed',
          status: 'completed',
        },
        { suppressCallback: true }
      );

      const eventQueries = createEventQueries(
        drizzle(instance.ctx.storage, { logger: false }),
        instance.ctx.storage.sql
      );
      const events = eventQueries.findByFilters({ eventTypes: ['message.completed'] });
      return { events, sentCallbackJobs };
    });

    expect(result.events).toHaveLength(0);
    expect(result.sentCallbackJobs).toHaveLength(0);
  });

  it('accepted execution completion callback includes messageId when present', async () => {
    const userId = 'user_accepted_callback_message';
    const sessionId = 'agent_accepted_callback_message';
    const stub = env.CLOUD_AGENT_SESSION.get(
      env.CLOUD_AGENT_SESSION.idFromName(`${userId}:${sessionId}`)
    );

    const result = await runInDurableObject(stub, async instance => {
      const sentCallbackJobs: Array<{
        payload: { executionId: string; messageId?: string; status: 'completed' };
      }> = [];

      await instance.prepare({
        sessionId,
        userId,
        prompt: 'prepared prompt',
        mode: 'code',
        model: 'test-model',
        kiloSessionId: '44444444-4444-4444-8444-444444444444',
        kilocodeToken: 'token-callback-message',
        callbackTarget: { url: 'https://example.com/callback' },
      });
      await instance.addExecution({
        executionId: 'exc_accepted_callback_message',
        mode: 'code',
        streamingMode: 'websocket',
        ingestToken: 'exc_accepted_callback_message',
        messageId: 'msg_018f1e2d3c4bCallMsgAbCdEfG',
      });

      const enqueueCallbackNotification = instance['enqueueCallbackNotification'].bind(
        instance
      ) as (
        execution: { executionId: string; messageId?: string },
        status: 'completed'
      ) => Promise<void>;
      instance['enqueueCallbackNotification'] = async (execution, status) => {
        const payload: { executionId: string; messageId?: string; status: 'completed' } = {
          executionId: execution.executionId,
          status,
        };
        if (execution.messageId) {
          payload.messageId = execution.messageId;
        }
        sentCallbackJobs.push({ payload });
        await enqueueCallbackNotification(execution, status);
      };

      await instance.updateExecutionStatus({
        executionId: 'exc_accepted_callback_message',
        status: 'running',
      });
      await instance.updateExecutionStatus({
        executionId: 'exc_accepted_callback_message',
        status: 'completed',
      });

      return sentCallbackJobs;
    });

    expect(result).toHaveLength(1);
    expect(result[0].payload).toMatchObject({
      executionId: 'exc_accepted_callback_message',
      messageId: 'msg_018f1e2d3c4bCallMsgAbCdEfG',
    });
  });

  it('prepared initial execution completion uses the prepared initialMessageId', async () => {
    const userId = 'user_prepared_initial_completed';
    const sessionId = 'agent_prepared_initial_completed';
    const stub = env.CLOUD_AGENT_SESSION.get(
      env.CLOUD_AGENT_SESSION.idFromName(`${userId}:${sessionId}`)
    );

    const result = await runInDurableObject(stub, async instance => {
      let capturedPlan: { executionId: string; messageId: string } | null = null;
      const sentCallbackJobs: Array<{ payload: { executionId: string; messageId?: string } }> = [];
      (instance as any).orchestrator = {
        execute: async (plan: any) => {
          capturedPlan = { executionId: plan.executionId, messageId: plan.messageId };
          return { messageId: plan.messageId, kiloSessionId: 'kilo_test' };
        },
      };
      instance['enqueueCallbackNotification'] = async (
        execution: { executionId: string; messageId?: string },
        _status: 'completed' | 'failed' | 'interrupted'
      ) => {
        sentCallbackJobs.push({
          payload: {
            executionId: execution.executionId,
            messageId: execution.messageId,
          },
        });
      };

      await instance.prepare({
        sessionId,
        userId,
        prompt: 'prepared prompt',
        mode: 'code',
        model: 'test-model',
        kiloSessionId: '55555555-5555-4555-8555-555555555555',
        kilocodeToken: 'token-prepared-initial',
        gitUrl: 'https://example.com/repo.git',
        gitToken: 'old-token',
        callbackTarget: { url: 'https://example.com/callback' },
        initialMessageId: 'msg_018f1e2d3c4bTermInitMsgABC',
      });

      const startResult = await instance.startExecutionV2({
        kind: 'initiatePrepared',
        userId,
        authToken: 'token-prepared-initial',
      });
      await instance.alarm();
      if (!capturedPlan) throw new Error('expected captured plan');
      await instance.updateExecutionStatus({
        executionId: capturedPlan.executionId,
        status: 'running',
      });
      await instance.updateExecutionStatus({
        executionId: capturedPlan.executionId,
        status: 'completed',
      });

      const eventQueries = createEventQueries(
        drizzle(instance.ctx.storage, { logger: false }),
        instance.ctx.storage.sql
      );
      const events = eventQueries.findByFilters({ eventTypes: ['message.completed'] });
      return { startResult, events, sentCallbackJobs };
    });

    expect(result.startResult.success).toBe(true);
    if (!result.startResult.success) return;
    expect(result.startResult.messageId).toBe('msg_018f1e2d3c4bTermInitMsgABC');
    expect(JSON.parse(result.events[0].payload)).toMatchObject({
      messageId: 'msg_018f1e2d3c4bTermInitMsgABC',
      executionId: result.startResult.executionId,
      status: 'completed',
    });
    expect(result.sentCallbackJobs[0]?.payload).toMatchObject({
      executionId: result.startResult.executionId,
      messageId: 'msg_018f1e2d3c4bTermInitMsgABC',
    });
  });

  it('accepted execution failure emits message.failed with accepted marker', async () => {
    const userId = 'user_accepted_failed';
    const sessionId = 'agent_accepted_failed';
    const stub = env.CLOUD_AGENT_SESSION.get(
      env.CLOUD_AGENT_SESSION.idFromName(`${userId}:${sessionId}`)
    );

    const result = await runInDurableObject(stub, async instance => {
      await instance.prepare({
        sessionId,
        userId,
        prompt: 'prepared prompt',
        mode: 'code',
        model: 'test-model',
        kilocodeToken: 'token-failed',
      });
      await instance.addExecution({
        executionId: 'exc_accepted_failed',
        mode: 'code',
        streamingMode: 'websocket',
        ingestToken: 'exc_accepted_failed',
        messageId: 'msg_018f1e2d3c4bAcceptFailAbCd',
      });

      const failed = await instance.failExecutionRpc({
        executionId: 'exc_accepted_failed',
        error: 'fatal failure',
        status: 'failed',
      });

      const eventQueries = createEventQueries(
        drizzle(instance.ctx.storage, { logger: false }),
        instance.ctx.storage.sql
      );
      const events = eventQueries.findByFilters({ eventTypes: ['message.failed'] });
      return { failed, events };
    });

    expect(result.failed).toBe(true);
    expect(result.events).toHaveLength(1);
    expect(JSON.parse(result.events[0].payload)).toEqual({
      messageId: 'msg_018f1e2d3c4bAcceptFailAbCd',
      executionId: 'exc_accepted_failed',
      status: 'failed',
      error: 'fatal failure',
      delivery: 'sent',
      accepted: true,
    });
  });

  it('completion schedules and advances the next pending message', async () => {
    const userId = 'user_pending_completion';
    const sessionId = 'agent_pending_completion';
    const stub = env.CLOUD_AGENT_SESSION.get(
      env.CLOUD_AGENT_SESSION.idFromName(`${userId}:${sessionId}`)
    );

    const result = await runInDurableObject(stub, async instance => {
      const acceptedMessageIds: string[] = [];
      (instance as any).orchestrator = {
        execute: async (plan: any) => {
          acceptedMessageIds.push(plan.messageId);
          return { messageId: plan.messageId, kiloSessionId: 'kilo_test' };
        },
      };
      await instance.prepare({
        sessionId,
        userId,
        kiloSessionId: '22222222-2222-4222-2222-222222222222',
        prompt: 'prepared prompt',
        mode: 'code',
        model: 'test-model',
        kilocodeToken: 'token-followup',
        gitUrl: 'https://example.com/repo.git',
        gitToken: 'old-token',
      });
      await instance.tryInitiate();
      await instance.addExecution({
        executionId: 'exc_completion_active',
        mode: 'code',
        streamingMode: 'websocket',
        ingestToken: 'exc_completion_active',
      });
      await instance.ctx.storage.put('wrapper_runtime_state', {
        wrapperGeneration: 1,
        wrapperConnectionId: 'conn_completion_active',
        wrapperExecutionId: 'exc_completion_active',
        acceptedExecutionId: 'exc_completion_active',
      });
      await storePendingSessionMessage(
        instance.ctx.storage,
        createMessage({
          messageId: 'msg_018f1e2d3c4bComplNextAbCdE',
          executionId: 'exc_completion_next',
          content: 'next message',
          createdAt: 1,
        })
      );

      await instance.onExecutionComplete('exc_completion_active', 'completed');
      await instance.alarm();
      const pending = await listPendingSessionMessages(instance.ctx.storage);
      const currentRuntimeExecution = await instance.getCurrentRuntimeExecution();
      return { acceptedMessageIds, pending, currentRuntimeExecution };
    });

    expect(result.acceptedMessageIds).toEqual(['msg_018f1e2d3c4bComplNextAbCdE']);
    expect(result.pending).toHaveLength(0);
    expect(result.currentRuntimeExecution?.executionId).toBe('exc_completion_next');
  });
});
