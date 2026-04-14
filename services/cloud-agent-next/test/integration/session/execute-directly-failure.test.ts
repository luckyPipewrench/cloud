/**
 * Integration test for the executeDirectly catch-block fix in CloudAgentSession.
 *
 * When the orchestrator throws during executeDirectly, the execution must be
 * marked as failed (with a callback notification enqueued) and a synthetic
 * error stream event must be persisted — before the error propagates to the
 * startExecutionV2 outer catch which returns { success: false }.
 */

import { env, runInDurableObject, listDurableObjectIds } from 'cloudflare:test';
import { describe, it, expect, beforeEach, vi } from 'vitest';
import { drizzle } from 'drizzle-orm/durable-sqlite';
import { createEventQueries } from '../../../src/session/queries/events.js';
import type { ExecutionId } from '../../../src/types/ids.js';
import type { ExecutionPlan, StartExecutionV2Request } from '../../../src/execution/types.js';
import { listPendingSessionMessages } from '../../../src/session/pending-messages.js';
import {
  getWrapperRuntimeState,
  recordWrapperPong,
} from '../../../src/session/wrapper-runtime-state.js';

describe('executeDirectly failure handling', () => {
  beforeEach(async () => {
    const ids = await listDurableObjectIds(env.CLOUD_AGENT_SESSION);
    expect(ids).toHaveLength(0);
  });

  it('terminal execution clears wrapper liveness deadlines before alarm reschedules', async () => {
    const userId = 'user_terminal_liveness';
    const sessionId = 'agent_terminal_liveness';
    const doId = env.CLOUD_AGENT_SESSION.idFromName(`${userId}:${sessionId}`);
    const stub = env.CLOUD_AGENT_SESSION.get(doId);

    const result = await runInDurableObject(stub, async instance => {
      await instance.prepare({
        sessionId,
        userId,
        orgId: 'org_terminal_liveness',
        kiloSessionId: 'bbbbbbbb-bbbb-4bbb-bbbb-bbbbbbbbbbbb',
        prompt: 'initial prompt',
        mode: 'code',
        model: 'test-model',
        kilocodeToken: 'token-terminal-liveness',
      });
      await instance.tryInitiate();

      const executionId = 'exec_terminal_liveness' as ExecutionId;
      const addResult = await instance.addExecution({
        executionId,
        mode: 'code',
        streamingMode: 'websocket',
        ingestToken: executionId,
        messageId: 'msg_018f1e2d3c4bTermRunAbCdEfG',
      });
      expect(addResult.ok).toBe(true);

      const state = await recordWrapperPong(
        instance.ctx.storage,
        0,
        'missing',
        Date.now(),
        Date.now() + 60_000
      );
      expect(state).toBeNull();

      await instance.ctx.storage.put('wrapper_runtime_state', {
        wrapperGeneration: 1,
        wrapperConnectionId: 'conn_terminal',
        wrapperExecutionId: executionId,
        acceptedMessageId: 'msg_018f1e2d3c4bTermRunAbCdEfG',
        acceptedExecutionId: executionId,
        noOutputDeadlineAt: Date.now() - 1,
        pingDeadlineAt: Date.now() - 1,
        nextPingAt: Date.now() - 1,
      });

      await instance.updateExecutionStatus({
        executionId,
        status: 'completed',
        completedAt: Date.now(),
      });
      await instance.alarm();

      return {
        currentRuntimeExecution: await instance.getCurrentRuntimeExecution(),
        wrapperRuntimeState: await getWrapperRuntimeState(instance.ctx.storage),
        alarm: await instance.ctx.storage.getAlarm(),
      };
    });

    expect(result.currentRuntimeExecution).toBeNull();
    expect(result.wrapperRuntimeState.wrapperGeneration).toBe(2);
    expect(result.wrapperRuntimeState.wrapperConnectionId).toBeUndefined();
    expect(result.wrapperRuntimeState.acceptedMessageId).toBeUndefined();
    expect(result.wrapperRuntimeState.acceptedExecutionId).toBeUndefined();
    expect(result.wrapperRuntimeState.noOutputDeadlineAt).toBeUndefined();
    expect(result.wrapperRuntimeState.pingDeadlineAt).toBeUndefined();
    expect(result.wrapperRuntimeState.nextPingAt).toBeUndefined();
    expect(result.alarm).toBeGreaterThan(Date.now() + 1_000);
  });

  it('wrapper heartbeat keeps accepted no-output liveness alive', async () => {
    const userId = 'user_liveness_heartbeat';
    const sessionId = 'agent_liveness_heartbeat';
    const doId = env.CLOUD_AGENT_SESSION.idFromName(`${userId}:${sessionId}`);
    const stub = env.CLOUD_AGENT_SESSION.get(doId);

    const result = await runInDurableObject(stub, async instance => {
      instance['stopCurrentWrapperProcess'] = async () => {
        throw new Error('should not stop wrapper');
      };

      await instance.prepare({
        sessionId,
        userId,
        orgId: 'org_liveness_heartbeat',
        kiloSessionId: 'c1c1c1c1-c1c1-41c1-81c1-c1c1c1c1c1c1',
        prompt: 'initial prompt',
        mode: 'code',
        model: 'test-model',
        kilocodeToken: 'token-liveness-heartbeat',
      });
      await instance.tryInitiate();

      const executionId = 'exec_liveness_heartbeat' as ExecutionId;
      await instance.addExecution({
        executionId,
        mode: 'code',
        streamingMode: 'websocket',
        ingestToken: executionId,
        messageId: 'msg_018f1e2d3c4bHeartRunAbCdEf',
      });
      await instance.ctx.storage.put('wrapper_runtime_state', {
        wrapperGeneration: 1,
        wrapperConnectionId: 'conn_heartbeat',
        wrapperExecutionId: executionId,
        acceptedMessageId: 'msg_018f1e2d3c4bHeartRunAbCdEf',
        acceptedExecutionId: executionId,
        noOutputDeadlineAt: Date.now() + 5_000,
        nextPingAt: Date.now() + 60_000,
      });
      await instance.updateExecutionStatus({ executionId, status: 'running' });

      const handler = await instance['getIngestHandler']();
      const ws = {
        deserializeAttachment: () => ({
          executionId,
          connectedAt: Date.now(),
          kiloSessionState: { captured: false },
          lastHeartbeatUpdate: 0,
          lastEventAtUpdate: 0,
          wrapperGeneration: 1,
          wrapperConnectionId: 'conn_heartbeat',
        }),
        serializeAttachment: () => {},
        send: () => {},
      } as unknown as WebSocket;

      await handler.handleIngestMessage(
        ws,
        JSON.stringify({
          streamEventType: 'heartbeat',
          data: {},
          timestamp: new Date().toISOString(),
        })
      );

      const execution = await instance.getExecution(executionId);
      const wrapperRuntimeState = await getWrapperRuntimeState(instance.ctx.storage);
      return { execution, wrapperRuntimeState };
    });

    expect(result.execution?.status).toBe('running');
    expect(result.wrapperRuntimeState.noOutputDeadlineAt).toBeUndefined();
  });

  it('wrapper no-output liveness failure fails accepted message before deadline cleanup and preserves queued messages', async () => {
    const userId = 'user_liveness_no_output';
    const sessionId = 'agent_liveness_no_output';
    const doId = env.CLOUD_AGENT_SESSION.idFromName(`${userId}:${sessionId}`);
    const stub = env.CLOUD_AGENT_SESSION.get(doId);

    const result = await runInDurableObject(stub, async (instance, state) => {
      const stoppedWrappers: string[] = [];
      instance['stopCurrentWrapperProcess'] = async () => {
        stoppedWrappers.push('stopped');
      };
      const interruptSpy = vi.spyOn(instance, 'interruptExecution');

      await instance.prepare({
        sessionId,
        userId,
        orgId: 'org_liveness_no_output',
        kiloSessionId: 'cccccccc-cccc-4ccc-cccc-cccccccccccc',
        prompt: 'initial prompt',
        mode: 'code',
        model: 'test-model',
        kilocodeToken: 'token-liveness-no-output',
      });
      await instance.tryInitiate();

      const executionId = 'exec_liveness_no_output' as ExecutionId;
      await instance.addExecution({
        executionId,
        mode: 'code',
        streamingMode: 'websocket',
        ingestToken: executionId,
        messageId: 'msg_018f1e2d3c4bNoOutputAbCdEf',
      });
      await instance.ctx.storage.put('pending_message:0000000000000001:queued', {
        messageId: 'msg_018f1e2d3c4bQueueSurvAbCdE',
        executionId: 'exec_queued_survives',
        role: 'user',
        content: 'queued survives',
        createdAt: Date.now(),
      });
      const expiredAt = Date.now() - 1;
      await instance.ctx.storage.put('wrapper_runtime_state', {
        wrapperGeneration: 1,
        wrapperConnectionId: 'conn_no_output',
        wrapperExecutionId: executionId,
        acceptedMessageId: 'msg_018f1e2d3c4bNoOutputAbCdEf',
        acceptedExecutionId: executionId,
        noOutputDeadlineAt: expiredAt,
        lastHeartbeatUpdate: expiredAt - 10 * 60_000,
      });
      await instance.updateExecutionStatus({ executionId, status: 'running' });
      await instance.updateExecutionHeartbeat(executionId, expiredAt - 10 * 60_000);

      await instance.alarm();

      const execution = await instance.getExecution(executionId);
      const pending = await listPendingSessionMessages(instance.ctx.storage);
      const db = drizzle(state.storage, { logger: false });
      const eventQueries = createEventQueries(db, state.storage.sql);
      const events = eventQueries.findByFilters({ executionIds: [executionId] });
      return {
        execution,
        pending,
        events,
        stoppedWrappers,
        interruptCalls: interruptSpy.mock.calls.length,
        alarm: await instance.ctx.storage.getAlarm(),
        wrapperRuntimeState: await getWrapperRuntimeState(instance.ctx.storage),
      };
    });

    expect(result.execution?.status).toBe('failed');
    expect(result.execution?.error).toBe('Wrapper accepted the message but produced no output');
    expect(result.pending.map(message => message.messageId)).toEqual([
      'msg_018f1e2d3c4bQueueSurvAbCdE',
    ]);
    expect(result.wrapperRuntimeState.wrapperConnectionId).toBeUndefined();
    expect(result.wrapperRuntimeState.noOutputDeadlineAt).toBeUndefined();
    expect(result.wrapperRuntimeState.pingDeadlineAt).toBeUndefined();
    expect(result.wrapperRuntimeState.nextPingAt).toBeUndefined();
    expect(result.alarm).toBeGreaterThanOrEqual(Date.now() + 900);
    expect(result.interruptCalls).toBe(0);
    expect(result.stoppedWrappers).toEqual(['stopped']);
    const failedEvents = result.events.filter(
      event => event.stream_event_type === 'message.failed'
    );
    expect(failedEvents).toHaveLength(1);
    expect(JSON.parse(failedEvents[0].payload)).toMatchObject({
      messageId: 'msg_018f1e2d3c4bNoOutputAbCdEf',
      executionId: 'exec_liveness_no_output',
      status: 'failed',
      error: 'Wrapper accepted the message but produced no output',
      delivery: 'sent',
      accepted: true,
    });
  });

  it('wrapper ping timeout liveness failure fails accepted message and preserves queued messages', async () => {
    const userId = 'user_liveness_ping';
    const sessionId = 'agent_liveness_ping';
    const doId = env.CLOUD_AGENT_SESSION.idFromName(`${userId}:${sessionId}`);
    const stub = env.CLOUD_AGENT_SESSION.get(doId);

    const result = await runInDurableObject(stub, async (instance, state) => {
      instance['stopCurrentWrapperProcess'] = async () => {};
      const interruptSpy = vi.spyOn(instance, 'interruptExecution');

      await instance.prepare({
        sessionId,
        userId,
        orgId: 'org_liveness_ping',
        kiloSessionId: 'dddddddd-dddd-4ddd-dddd-dddddddddddd',
        prompt: 'initial prompt',
        mode: 'code',
        model: 'test-model',
        kilocodeToken: 'token-liveness-ping',
      });
      await instance.tryInitiate();

      const executionId = 'exec_liveness_ping' as ExecutionId;
      await instance.addExecution({
        executionId,
        mode: 'code',
        streamingMode: 'websocket',
        ingestToken: executionId,
        messageId: 'msg_018f1e2d3c4bPingFailAbCdEf',
      });
      await instance.ctx.storage.put(
        'pending_message:0000000000000001:msg_018f1e2d3c4bQueueSurvAbCdE',
        {
          messageId: 'msg_018f1e2d3c4bQueueSurvAbCdE',
          executionId: 'exec_queued_survives',
          role: 'user',
          content: 'queued survives',
          createdAt: Date.now(),
        }
      );
      await instance.ctx.storage.put('wrapper_runtime_state', {
        wrapperGeneration: 1,
        wrapperConnectionId: 'conn_ping',
        wrapperExecutionId: executionId,
        acceptedMessageId: 'msg_018f1e2d3c4bPingFailAbCdEf',
        acceptedExecutionId: executionId,
        pingDeadlineAt: Date.now() - 1,
      });
      await instance.updateExecutionStatus({ executionId, status: 'running' });

      await instance.alarm();

      const execution = await instance.getExecution(executionId);
      const pending = await listPendingSessionMessages(instance.ctx.storage);
      const db = drizzle(state.storage, { logger: false });
      const eventQueries = createEventQueries(db, state.storage.sql);
      const events = eventQueries.findByFilters({ executionIds: [executionId] });
      return {
        execution,
        pending,
        events,
        interruptCalls: interruptSpy.mock.calls.length,
        alarm: await instance.ctx.storage.getAlarm(),
        wrapperRuntimeState: await getWrapperRuntimeState(instance.ctx.storage),
      };
    });

    expect(result.execution?.status).toBe('failed');
    expect(result.execution?.error).toBe('Wrapper did not respond to liveness ping');
    expect(result.pending.map(message => message.messageId)).toEqual([
      'msg_018f1e2d3c4bQueueSurvAbCdE',
    ]);
    expect(result.wrapperRuntimeState.wrapperConnectionId).toBeUndefined();
    expect(result.wrapperRuntimeState.pingDeadlineAt).toBeUndefined();
    expect(result.wrapperRuntimeState.nextPingAt).toBeUndefined();
    expect(result.alarm).toBeGreaterThanOrEqual(Date.now() + 900);
    expect(result.interruptCalls).toBe(0);
    const failedEvents = result.events.filter(
      event => event.stream_event_type === 'message.failed'
    );
    expect(failedEvents).toHaveLength(1);
    expect(JSON.parse(failedEvents[0].payload)).toMatchObject({
      messageId: 'msg_018f1e2d3c4bPingFailAbCdEf',
      executionId: 'exec_liveness_ping',
      status: 'failed',
      error: 'Wrapper did not respond to liveness ping',
      delivery: 'sent',
      accepted: true,
    });
  });

  it('queued flush pre-start failure retries cleanly with the original execution and message ids', async () => {
    const userId = 'user_exec_direct_fail';
    const sessionId = 'agent_exec_direct_fail';
    const doId = env.CLOUD_AGENT_SESSION.idFromName(`${userId}:${sessionId}`);
    const stub = env.CLOUD_AGENT_SESSION.get(doId);

    const result = await runInDurableObject(stub, async (instance, state) => {
      let attemptCount = 0;
      (instance as any).orchestrator = {
        execute: async (plan: ExecutionPlan) => {
          attemptCount += 1;
          if (attemptCount === 1) {
            throw new Error('Sandbox connect failed');
          }

          return { messageId: plan.messageId, kiloSessionId: 'kilo_retry_success' };
        },
      };

      await instance.prepare({
        sessionId,
        userId,
        orgId: 'org_exec_direct_fail',
        kiloSessionId: 'aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaaa',
        prompt: 'initial prompt',
        mode: 'code',
        model: 'test-model',
        kilocodeToken: 'token-direct-fail',
        gitUrl: 'https://example.com/repo.git',
        gitToken: 'git-token',
      });

      await instance.tryInitiate();

      const request: StartExecutionV2Request = {
        kind: 'followup',
        userId,
        prompt: 'do some work',
        messageId: 'msg_018f1e2d3c4bFailMsgAbCdEfG',
      };

      const startResult = await instance.startExecutionV2(request);
      const pendingAfterStart = await listPendingSessionMessages(instance.ctx.storage);

      await instance.alarm();

      const pendingAfterAlarm = await listPendingSessionMessages(instance.ctx.storage);
      const executionsAfterFirstAlarm = await instance.getExecutions();
      const wrapperRuntimeState = await getWrapperRuntimeState(instance.ctx.storage);

      const retriableMessage = pendingAfterAlarm[0];
      if (retriableMessage) {
        await instance.ctx.storage.put('pending_message:0000000000000001:retry-fix', {
          ...retriableMessage,
          nextFlushAttemptAt: Date.now() - 1,
        });
        await instance.ctx.storage.delete(
          'pending_message:0000000000000001:msg_018f1e2d3c4bFailMsgAbCdEfG'
        );
      }

      await instance.alarm();

      const currentRuntimeExecution = await instance.getCurrentRuntimeExecution();
      const pendingAfterRetry = await listPendingSessionMessages(instance.ctx.storage);
      const executionsAfterRetry = await instance.getExecutions();

      const db = drizzle(state.storage, { logger: false });
      const eventQueries = createEventQueries(db, state.storage.sql);
      const retryEvents = eventQueries.findByFilters({
        executionIds: [startResult.success ? startResult.executionId : ('missing' as ExecutionId)],
      });

      return {
        startResult,
        attemptCount,
        pendingAfterStart,
        pendingAfterAlarm,
        pendingAfterRetry,
        executionsAfterFirstAlarm,
        executionsAfterRetry,
        currentRuntimeExecution,
        wrapperRuntimeState,
        retryEvents,
      };
    });

    expect(result.startResult.success).toBe(true);
    if (!result.startResult.success) return;
    expect(result.startResult.delivery).toBe('queued');
    expect(result.pendingAfterStart.map(message => message.messageId)).toEqual([
      'msg_018f1e2d3c4bFailMsgAbCdEfG',
    ]);

    expect(result.pendingAfterAlarm.map(message => message.messageId)).toEqual([
      'msg_018f1e2d3c4bFailMsgAbCdEfG',
    ]);
    expect(result.pendingAfterAlarm[0]?.executionId).toBe(result.startResult.executionId);
    expect(result.pendingAfterAlarm[0]?.lastFlushError).toBe('Sandbox connect failed');
    expect(result.executionsAfterFirstAlarm).toEqual([]);
    expect(result.wrapperRuntimeState.wrapperGeneration).toBe(2);
    expect(result.wrapperRuntimeState.wrapperConnectionId).toBeUndefined();
    expect(result.wrapperRuntimeState.wrapperExecutionId).toBeUndefined();

    expect(result.attemptCount).toBe(2);
    expect(result.pendingAfterRetry).toHaveLength(0);
    expect(result.currentRuntimeExecution?.executionId).toBe(result.startResult.executionId);
    expect(result.currentRuntimeExecution?.messageId).toBe('msg_018f1e2d3c4bFailMsgAbCdEfG');
    expect(result.executionsAfterRetry).toHaveLength(1);
    expect(result.executionsAfterRetry[0]?.executionId).toBe(result.startResult.executionId);
    expect(result.executionsAfterRetry[0]?.status).toBe('pending');
    expect(result.executionsAfterRetry.some(execution => execution.status === 'failed')).toBe(
      false
    );
    expect(result.retryEvents.filter(event => event.stream_event_type === 'error')).toHaveLength(0);
  });
});
