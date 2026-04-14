/* eslint-disable @typescript-eslint/unbound-method */
import { describe, expect, it, vi } from 'vitest';
import { createIngestHandler, type IngestDOContext, type IngestAttachment } from './ingest.js';
import type { EventQueries } from '../session/queries/index.js';
import type { SessionId, ExecutionId } from '../types/ids.js';

const SESSION_ID = 'sess_test' as SessionId;
const EXECUTION_ID = 'exc_test' as ExecutionId;
const itWithWebSocketPair = typeof WebSocketPair === 'undefined' ? it.skip : it;

function createFakeState() {
  return {
    acceptWebSocket: vi.fn(),
    getWebSockets: vi.fn().mockReturnValue([]),
    getTags: vi.fn().mockReturnValue([]),
  } as unknown as DurableObjectState;
}

function makeIngestRequest(params: Record<string, string>) {
  const url = new URL('https://example.com/ingest');
  for (const [key, value] of Object.entries(params)) {
    url.searchParams.set(key, value);
  }
  return new Request(url, { headers: { Upgrade: 'websocket' } });
}

function createFakeEventQueries() {
  return {
    insert: vi.fn().mockReturnValue(1),
    findByFilters: vi.fn().mockReturnValue([]),
    deleteOlderThan: vi.fn().mockReturnValue(0),
    iterateByFilters: vi.fn(),
    countByExecutionId: vi.fn(),
    getLatestEventId: vi.fn(),
  } as unknown as EventQueries;
}

function createFakeDOContext(): IngestDOContext {
  return {
    updateKiloSessionId: vi.fn().mockResolvedValue(undefined),
    updateUpstreamBranch: vi.fn().mockResolvedValue(undefined),
    getExecution: vi.fn().mockResolvedValue(null),
    transitionToRunning: vi.fn().mockResolvedValue(true),
    updateHeartbeat: vi.fn().mockResolvedValue(undefined),
    updateLastEventAt: vi.fn().mockResolvedValue(undefined),
    updateExecutionStatus: vi.fn().mockResolvedValue(undefined),
  };
}

function createFakeWebSocket(attachment: unknown = null) {
  return {
    deserializeAttachment: vi.fn().mockReturnValue(attachment),
    serializeAttachment: vi.fn(),
    send: vi.fn(),
    close: vi.fn(),
    readyState: 1,
  } as unknown as WebSocket;
}

function makeAttachment(overrides?: Partial<IngestAttachment>): IngestAttachment {
  const now = Date.now();
  return {
    executionId: EXECUTION_ID,
    connectedAt: now,
    kiloSessionState: { captured: false },
    lastHeartbeatUpdate: now,
    lastEventAtUpdate: now,
    ...overrides,
  };
}

function makeStreamMessage(streamEventType: string, data?: Record<string, unknown>) {
  return JSON.stringify({
    streamEventType,
    data: data ?? {},
    timestamp: new Date().toISOString(),
  });
}

describe('createIngestHandler', () => {
  describe('handleIngestClose', () => {
    it('returns null when WebSocket has no attachment', async () => {
      const state = createFakeState();
      const handler = createIngestHandler(
        state,
        createFakeEventQueries(),
        SESSION_ID,
        vi.fn(),
        createFakeDOContext()
      );
      const ws = createFakeWebSocket(null);

      await expect(handler.handleIngestClose(ws)).resolves.toBeNull();
    });

    it('returns executionId when no other ingest sockets remain', async () => {
      const state = createFakeState();
      // No remaining sockets for this execution
      vi.mocked(state.getWebSockets).mockReturnValue([]);

      const handler = createIngestHandler(
        state,
        createFakeEventQueries(),
        SESSION_ID,
        vi.fn(),
        createFakeDOContext()
      );
      const ws = createFakeWebSocket(makeAttachment());

      await expect(handler.handleIngestClose(ws)).resolves.toEqual({
        executionId: EXECUTION_ID,
        wrapperGeneration: undefined,
        wrapperConnectionId: undefined,
      });
      expect(state.getWebSockets).toHaveBeenCalledWith(`ingest:${EXECUTION_ID}`);
    });

    it('returns null when a replacement ingest socket exists', async () => {
      const state = createFakeState();
      const replacementWs = createFakeWebSocket();
      // A replacement socket still exists for this execution
      vi.mocked(state.getWebSockets).mockReturnValue([replacementWs]);

      const handler = createIngestHandler(
        state,
        createFakeEventQueries(),
        SESSION_ID,
        vi.fn(),
        createFakeDOContext()
      );
      const ws = createFakeWebSocket(makeAttachment());

      await expect(handler.handleIngestClose(ws)).resolves.toBeNull();
    });

    // The positive case through the full handleIngestRequest → handleIngestClose
    // flow requires WebSocketPair and state.acceptWebSocket — Cloudflare Worker
    // APIs unavailable in vitest Node. That path is covered by integration tests.
  });

  describe('handleIngestMessage — persistence routing', () => {
    function makeKilocodeMessage(eventName: string, properties?: Record<string, unknown>) {
      return JSON.stringify({
        streamEventType: 'kilocode',
        data: { event: eventName, properties },
        timestamp: new Date().toISOString(),
      });
    }

    // --- kilocode events: upsert path ---

    it('message.updated is upserted by entity ID', async () => {
      const eventQueries = createFakeEventQueries();
      (eventQueries as unknown as Record<string, unknown>).upsert = vi.fn().mockReturnValue(42);
      const broadcastFn = vi.fn();
      const handler = createIngestHandler(
        createFakeState(),
        eventQueries,
        SESSION_ID,
        broadcastFn,
        createFakeDOContext()
      );
      const ws = createFakeWebSocket(makeAttachment());

      await handler.handleIngestMessage(
        ws,
        makeKilocodeMessage('message.updated', { info: { id: 'msg_1' } })
      );

      expect(eventQueries.insert).not.toHaveBeenCalled();
      expect(
        (eventQueries as unknown as Record<string, ReturnType<typeof vi.fn>>).upsert
      ).toHaveBeenCalledWith(expect.objectContaining({ entityId: 'message/msg_1' }));
      expect(broadcastFn).toHaveBeenCalledWith(expect.objectContaining({ id: 42 }));
    });

    // --- kilocode events: plain insert path (PERSISTED_KILO_EVENT_NAMES) ---

    it.each([
      'message.part.removed',
      'session.created',
      'session.updated',
      'session.status',
      'session.error',
      'session.idle',
      'session.turn.close',
    ])('kilocode %s is plain-inserted', async eventName => {
      const eventQueries = createFakeEventQueries();
      const broadcastFn = vi.fn();
      const handler = createIngestHandler(
        createFakeState(),
        eventQueries,
        SESSION_ID,
        broadcastFn,
        createFakeDOContext()
      );
      const ws = createFakeWebSocket(makeAttachment());

      await handler.handleIngestMessage(ws, makeKilocodeMessage(eventName));

      expect(eventQueries.insert).toHaveBeenCalled();
      expect(broadcastFn).toHaveBeenCalledWith(expect.objectContaining({ id: 1 }));
    });

    // --- kilocode events: broadcast-only (not in any allowlist) ---

    it.each([
      'question.asked',
      'question.replied',
      'question.rejected',
      'session.diff',
      'message.part.delta',
      'permission.asked',
      'session.completed',
    ])('kilocode %s is broadcast-only', async eventName => {
      const eventQueries = createFakeEventQueries();
      const broadcastFn = vi.fn();
      const handler = createIngestHandler(
        createFakeState(),
        eventQueries,
        SESSION_ID,
        broadcastFn,
        createFakeDOContext()
      );
      const ws = createFakeWebSocket(makeAttachment());

      await handler.handleIngestMessage(ws, makeKilocodeMessage(eventName));

      expect(eventQueries.insert).not.toHaveBeenCalled();
      expect(broadcastFn).toHaveBeenCalledWith(
        expect.objectContaining({ id: 0, stream_event_type: 'kilocode' })
      );
    });

    // --- non-kilocode: plain insert path (PERSISTED_STREAM_EVENT_TYPES) ---

    it.each(['complete', 'interrupted', 'error', 'autocommit_started', 'autocommit_completed'])(
      'stream event %s is plain-inserted',
      async eventType => {
        const eventQueries = createFakeEventQueries();
        const broadcastFn = vi.fn();
        const handler = createIngestHandler(
          createFakeState(),
          eventQueries,
          SESSION_ID,
          broadcastFn,
          createFakeDOContext()
        );
        const ws = createFakeWebSocket(makeAttachment());

        await handler.handleIngestMessage(ws, makeStreamMessage(eventType));

        expect(eventQueries.insert).toHaveBeenCalled();
        expect(broadcastFn).toHaveBeenCalledWith(expect.objectContaining({ id: 1 }));
      }
    );

    // --- non-kilocode: broadcast-only (not in PERSISTED_STREAM_EVENT_TYPES) ---

    it.each(['heartbeat', 'pong', 'output', 'status', 'started', 'wrapper_resumed'])(
      'stream event %s is broadcast-only',
      async eventType => {
        const eventQueries = createFakeEventQueries();
        const broadcastFn = vi.fn();
        const handler = createIngestHandler(
          createFakeState(),
          eventQueries,
          SESSION_ID,
          broadcastFn,
          createFakeDOContext()
        );
        const ws = createFakeWebSocket(makeAttachment());

        await handler.handleIngestMessage(ws, makeStreamMessage(eventType));

        expect(eventQueries.insert).not.toHaveBeenCalled();
        expect(broadcastFn).toHaveBeenCalledWith(
          expect.objectContaining({ id: 0, stream_event_type: eventType })
        );
      }
    );

    it('pong updates wrapper liveness before broadcast without marking meaningful output', async () => {
      const calls: string[] = [];
      const doContext = createFakeDOContext();
      doContext.isCurrentWrapperConnection = vi.fn().mockResolvedValue(true);
      doContext.recordWrapperPong = vi.fn().mockImplementation(async () => {
        calls.push('pong');
      });
      doContext.recordMeaningfulWrapperOutput = vi.fn().mockResolvedValue(undefined);
      const handler = createIngestHandler(
        createFakeState(),
        createFakeEventQueries(),
        SESSION_ID,
        () => calls.push('broadcast'),
        doContext
      );
      const ws = createFakeWebSocket(
        makeAttachment({ wrapperGeneration: 3, wrapperConnectionId: 'conn_current' })
      );

      await handler.handleIngestMessage(ws, makeStreamMessage('pong'));

      expect(doContext.recordWrapperPong).toHaveBeenCalledWith(
        3,
        'conn_current',
        expect.any(Number)
      );
      expect(doContext.recordMeaningfulWrapperOutput).not.toHaveBeenCalled();
      expect(calls).toEqual(['pong', 'broadcast']);
    });

    it.each(['pong', 'wrapper_resumed'])(
      '%s does not clear no-output liveness',
      async eventType => {
        const doContext = createFakeDOContext();
        doContext.isCurrentWrapperConnection = vi.fn().mockResolvedValue(true);
        doContext.recordWrapperPong = vi.fn().mockResolvedValue(undefined);
        doContext.recordMeaningfulWrapperOutput = vi.fn().mockResolvedValue(undefined);
        const handler = createIngestHandler(
          createFakeState(),
          createFakeEventQueries(),
          SESSION_ID,
          vi.fn(),
          doContext
        );
        const ws = createFakeWebSocket(
          makeAttachment({ wrapperGeneration: 3, wrapperConnectionId: 'conn_current' })
        );

        await handler.handleIngestMessage(ws, makeStreamMessage(eventType));

        if (eventType === 'pong') {
          expect(doContext.recordWrapperPong).toHaveBeenCalled();
        } else {
          expect(doContext.recordWrapperPong).not.toHaveBeenCalled();
        }
        expect(doContext.recordMeaningfulWrapperOutput).not.toHaveBeenCalled();
      }
    );

    it('heartbeat clears no-output liveness before broadcast', async () => {
      const calls: string[] = [];
      const doContext = createFakeDOContext();
      doContext.isCurrentWrapperConnection = vi.fn().mockResolvedValue(true);
      doContext.recordWrapperPong = vi.fn().mockResolvedValue(undefined);
      doContext.recordMeaningfulWrapperOutput = vi.fn().mockImplementation(async () => {
        calls.push('heartbeat');
      });
      const handler = createIngestHandler(
        createFakeState(),
        createFakeEventQueries(),
        SESSION_ID,
        () => calls.push('broadcast'),
        doContext
      );
      const ws = createFakeWebSocket(
        makeAttachment({ wrapperGeneration: 3, wrapperConnectionId: 'conn_current' })
      );

      await handler.handleIngestMessage(ws, makeStreamMessage('heartbeat'));

      expect(doContext.recordWrapperPong).not.toHaveBeenCalled();
      expect(doContext.recordMeaningfulWrapperOutput).toHaveBeenCalledWith(
        3,
        'conn_current',
        expect.any(Number)
      );
      expect(calls).toEqual(['heartbeat', 'broadcast']);
    });

    it('meaningful output clears no-output liveness before broadcast', async () => {
      const calls: string[] = [];
      const doContext = createFakeDOContext();
      doContext.isCurrentWrapperConnection = vi.fn().mockResolvedValue(true);
      doContext.recordMeaningfulWrapperOutput = vi.fn().mockImplementation(async () => {
        calls.push('meaningful');
      });
      const handler = createIngestHandler(
        createFakeState(),
        createFakeEventQueries(),
        SESSION_ID,
        () => calls.push('broadcast'),
        doContext
      );
      const ws = createFakeWebSocket(
        makeAttachment({ wrapperGeneration: 3, wrapperConnectionId: 'conn_current' })
      );

      await handler.handleIngestMessage(ws, makeStreamMessage('output'));

      expect(doContext.recordMeaningfulWrapperOutput).toHaveBeenCalledWith(
        3,
        'conn_current',
        expect.any(Number)
      );
      expect(calls).toEqual(['meaningful', 'broadcast']);
    });

    it('kilo_snapshot is broadcast-only (no special handling)', async () => {
      const eventQueries = createFakeEventQueries();
      const broadcastFn = vi.fn();
      const doContext = createFakeDOContext();
      const handler = createIngestHandler(
        createFakeState(),
        eventQueries,
        SESSION_ID,
        broadcastFn,
        doContext
      );
      const ws = createFakeWebSocket(makeAttachment());

      await handler.handleIngestMessage(
        ws,
        JSON.stringify({
          streamEventType: 'kilo_snapshot',
          data: { sessionStatus: { type: 'busy' } },
          timestamp: new Date().toISOString(),
        })
      );

      // Should NOT call onKiloSnapshot (removed)
      // Should be broadcast as a regular event with eventId 0
      expect(broadcastFn).toHaveBeenCalledWith(
        expect.objectContaining({ id: 0, stream_event_type: 'kilo_snapshot' })
      );
    });
  });

  describe('wrapper fencing', () => {
    it('ignores stale fenced socket messages', async () => {
      const eventQueries = createFakeEventQueries();
      const broadcastFn = vi.fn();
      const doContext = createFakeDOContext();
      doContext.isCurrentWrapperConnection = vi.fn().mockResolvedValue(false);
      const handler = createIngestHandler(
        createFakeState(),
        eventQueries,
        SESSION_ID,
        broadcastFn,
        doContext
      );
      const ws = createFakeWebSocket(
        makeAttachment({ wrapperGeneration: 1, wrapperConnectionId: 'conn_old' })
      );

      await handler.handleIngestMessage(ws, makeStreamMessage('complete'));

      expect(eventQueries.insert).not.toHaveBeenCalled();
      expect(broadcastFn).not.toHaveBeenCalled();
      expect(doContext.updateExecutionStatus).not.toHaveBeenCalled();
    });

    it('does not report stale fenced socket close as disconnect', async () => {
      const doContext = createFakeDOContext();
      doContext.isCurrentWrapperConnection = vi.fn().mockResolvedValue(false);
      const handler = createIngestHandler(
        createFakeState(),
        createFakeEventQueries(),
        SESSION_ID,
        vi.fn(),
        doContext
      );
      const ws = createFakeWebSocket(
        makeAttachment({ wrapperGeneration: 1, wrapperConnectionId: 'conn_old' })
      );

      await expect(handler.handleIngestClose(ws)).resolves.toBeNull();
    });

    it('rejects malformed partial fenced connect params', async () => {
      const doContext = createFakeDOContext();
      doContext.getExecution = vi.fn().mockResolvedValue({
        executionId: EXECUTION_ID,
        ingestToken: EXECUTION_ID,
        status: 'pending',
      });
      const handler = createIngestHandler(
        createFakeState(),
        createFakeEventQueries(),
        SESSION_ID,
        vi.fn(),
        doContext
      );

      await expect(
        handler.handleIngestRequest(
          makeIngestRequest({ executionId: EXECUTION_ID, wrapperGeneration: '1' })
        )
      ).resolves.toMatchObject({ status: 400 });
      await expect(
        handler.handleIngestRequest(
          makeIngestRequest({ executionId: EXECUTION_ID, wrapperConnectionId: 'conn_current' })
        )
      ).resolves.toMatchObject({ status: 400 });
      await expect(
        handler.handleIngestRequest(
          makeIngestRequest({
            executionId: EXECUTION_ID,
            wrapperGeneration: 'not-a-number',
            wrapperConnectionId: 'conn_current',
          })
        )
      ).resolves.toMatchObject({ status: 400 });
    });

    it('rejects stale fenced connect params before accepting websocket', async () => {
      const state = createFakeState();
      const doContext = createFakeDOContext();
      doContext.getExecution = vi.fn().mockResolvedValue({
        executionId: EXECUTION_ID,
        ingestToken: EXECUTION_ID,
        status: 'pending',
      });
      doContext.isCurrentWrapperConnection = vi.fn().mockResolvedValue(false);
      const handler = createIngestHandler(
        state,
        createFakeEventQueries(),
        SESSION_ID,
        vi.fn(),
        doContext
      );

      const response = await handler.handleIngestRequest(
        makeIngestRequest({
          executionId: EXECUTION_ID,
          wrapperGeneration: '1',
          wrapperConnectionId: 'conn_old',
        })
      );

      expect(response.status).toBe(409);
      expect(state.acceptWebSocket).not.toHaveBeenCalled();
    });

    itWithWebSocketPair(
      'accepts current fenced connection and cancels matching grace',
      async () => {
        const state = createFakeState();
        const doContext = createFakeDOContext();
        doContext.getExecution = vi.fn().mockResolvedValue({
          executionId: EXECUTION_ID,
          ingestToken: EXECUTION_ID,
          status: 'pending',
        });
        doContext.isCurrentWrapperConnection = vi.fn().mockResolvedValue(true);
        doContext.cancelDisconnectGrace = vi.fn().mockResolvedValue(undefined);
        const handler = createIngestHandler(
          state,
          createFakeEventQueries(),
          SESSION_ID,
          vi.fn(),
          doContext
        );

        const response = await handler.handleIngestRequest(
          makeIngestRequest({
            executionId: EXECUTION_ID,
            wrapperGeneration: '2',
            wrapperConnectionId: 'conn_current',
          })
        );

        expect(response.status).toBe(101);
        expect(state.acceptWebSocket).toHaveBeenCalledOnce();
        expect(doContext.cancelDisconnectGrace).toHaveBeenCalledWith(2, 'conn_current');
      }
    );

    itWithWebSocketPair('replaces duplicate same fenced reconnect', async () => {
      const existingWs = createFakeWebSocket(
        makeAttachment({ wrapperGeneration: 2, wrapperConnectionId: 'conn_current' })
      );
      const state = createFakeState();
      vi.mocked(state.getWebSockets).mockReturnValue([existingWs]);
      const doContext = createFakeDOContext();
      doContext.getExecution = vi.fn().mockResolvedValue({
        executionId: EXECUTION_ID,
        ingestToken: EXECUTION_ID,
        status: 'running',
      });
      doContext.isCurrentWrapperConnection = vi.fn().mockResolvedValue(true);
      const handler = createIngestHandler(
        state,
        createFakeEventQueries(),
        SESSION_ID,
        vi.fn(),
        doContext
      );

      const response = await handler.handleIngestRequest(
        makeIngestRequest({
          executionId: EXECUTION_ID,
          wrapperGeneration: '2',
          wrapperConnectionId: 'conn_current',
        })
      );

      expect(response.status).toBe(101);
      expect(existingWs.close).toHaveBeenCalledWith(1000, 'Replaced by new connection');
    });

    itWithWebSocketPair(
      'does not let legacy reconnect replace fenced socket or cancel fenced grace',
      async () => {
        const existingWs = createFakeWebSocket(
          makeAttachment({ wrapperGeneration: 2, wrapperConnectionId: 'conn_current' })
        );
        const state = createFakeState();
        vi.mocked(state.getWebSockets).mockReturnValue([existingWs]);
        const doContext = createFakeDOContext();
        doContext.getExecution = vi.fn().mockResolvedValue({
          executionId: EXECUTION_ID,
          ingestToken: EXECUTION_ID,
          status: 'running',
        });
        doContext.cancelDisconnectGrace = vi.fn().mockResolvedValue(undefined);
        const handler = createIngestHandler(
          state,
          createFakeEventQueries(),
          SESSION_ID,
          vi.fn(),
          doContext
        );

        const response = await handler.handleIngestRequest(
          makeIngestRequest({ executionId: EXECUTION_ID })
        );

        expect(response.status).toBe(101);
        expect(existingWs.close).not.toHaveBeenCalled();
        expect(doContext.cancelDisconnectGrace).toHaveBeenCalledWith(undefined, undefined);
      }
    );
  });

  describe('hasActiveConnection', () => {
    it('returns true when getWebSockets finds ingest sockets', () => {
      const state = createFakeState();
      vi.mocked(state.getWebSockets).mockReturnValue([createFakeWebSocket()]);

      const handler = createIngestHandler(
        state,
        createFakeEventQueries(),
        SESSION_ID,
        vi.fn(),
        createFakeDOContext()
      );

      expect(handler.hasActiveConnection(EXECUTION_ID)).toBe(true);
      expect(state.getWebSockets).toHaveBeenCalledWith(`ingest:${EXECUTION_ID}`);
    });

    it('returns false when getWebSockets finds no ingest sockets', () => {
      const state = createFakeState();
      vi.mocked(state.getWebSockets).mockReturnValue([]);

      const handler = createIngestHandler(
        state,
        createFakeEventQueries(),
        SESSION_ID,
        vi.fn(),
        createFakeDOContext()
      );

      expect(handler.hasActiveConnection(EXECUTION_ID)).toBe(false);
    });
  });

  describe('handleIngestMessage — lastEventAt tracking', () => {
    it('calls updateLastEventAt for non-heartbeat events when debounce elapsed', async () => {
      const state = createFakeState();
      const doContext = createFakeDOContext();
      const broadcast = vi.fn();
      const handler = createIngestHandler(
        state,
        createFakeEventQueries(),
        SESSION_ID,
        broadcast,
        doContext
      );

      const staleTime = Date.now() - 31_000; // 31s ago — past HEARTBEAT_DEBOUNCE_MS
      const ws = createFakeWebSocket(makeAttachment({ lastEventAtUpdate: staleTime }));

      const message = JSON.stringify({
        streamEventType: 'kilocode',
        data: { event: 'message.updated' },
        timestamp: new Date().toISOString(),
      });

      await handler.handleIngestMessage(ws, message);

      expect(doContext.updateLastEventAt).toHaveBeenCalledWith(EXECUTION_ID, expect.any(Number));
    });

    it('does NOT call updateLastEventAt for heartbeat events', async () => {
      const state = createFakeState();
      const doContext = createFakeDOContext();
      const broadcast = vi.fn();
      const handler = createIngestHandler(
        state,
        createFakeEventQueries(),
        SESSION_ID,
        broadcast,
        doContext
      );

      const staleTime = Date.now() - 31_000;
      const ws = createFakeWebSocket(makeAttachment({ lastEventAtUpdate: staleTime }));

      const message = JSON.stringify({
        streamEventType: 'heartbeat',
        data: { executionId: EXECUTION_ID },
        timestamp: new Date().toISOString(),
      });

      await handler.handleIngestMessage(ws, message);

      expect(doContext.updateLastEventAt).not.toHaveBeenCalled();
    });

    it('debounces updateLastEventAt calls within 30s', async () => {
      const state = createFakeState();
      const doContext = createFakeDOContext();
      const broadcast = vi.fn();
      const handler = createIngestHandler(
        state,
        createFakeEventQueries(),
        SESSION_ID,
        broadcast,
        doContext
      );

      // Recent lastEventAtUpdate — within debounce window
      const recentTime = Date.now() - 5_000; // 5s ago — within HEARTBEAT_DEBOUNCE_MS
      const ws = createFakeWebSocket(makeAttachment({ lastEventAtUpdate: recentTime }));

      const message = JSON.stringify({
        streamEventType: 'kilocode',
        data: { event: 'message.updated' },
        timestamp: new Date().toISOString(),
      });

      await handler.handleIngestMessage(ws, message);

      expect(doContext.updateLastEventAt).not.toHaveBeenCalled();
    });
  });
});
