import { TRPCError } from '@trpc/server';
import { protectedProcedure } from '../auth.js';
import { withDORetry } from '../../utils/do-retry.js';
import type { SessionId } from '../../types/ids.js';
import type {
  StartExecutionV2Request,
  StartExecutionV2Result,
  RetryableResultCode,
} from '../../execution/types.js';
import { logger, withLogTags } from '../../logger.js';
import {
  InitiateFromPreparedSessionInput,
  SendMessageV2Input,
  type QueueAckResponse,
} from '../schemas.js';
import type { CloudAgentSession } from '../../persistence/CloudAgentSession.js';
import { createMessageId } from '../../session/message-id.js';

/** Retryable error codes that should map to 503 */
const RETRYABLE_CODES: readonly RetryableResultCode[] = [
  'SANDBOX_CONNECT_FAILED',
  'WORKSPACE_SETUP_FAILED',
  'KILO_SERVER_FAILED',
  'WRAPPER_START_FAILED',
] as const;

function isRetryableCode(code: string): code is RetryableResultCode {
  return RETRYABLE_CODES.includes(code as RetryableResultCode);
}

function throwStartExecutionError(
  result: Extract<StartExecutionV2Result, { success: false }>
): never {
  // Handle retryable errors as 503 Service Unavailable with specific error code
  if (isRetryableCode(result.code)) {
    throw new TRPCError({
      code: 'SERVICE_UNAVAILABLE',
      message: result.error,
      cause: {
        error: result.code,
        message: result.error,
        retryable: true,
      },
    });
  }

  const code =
    result.code === 'NOT_FOUND'
      ? 'NOT_FOUND'
      : result.code === 'BAD_REQUEST'
        ? 'BAD_REQUEST'
        : result.code === 'PENDING_QUEUE_FULL'
          ? 'TOO_MANY_REQUESTS'
          : 'INTERNAL_SERVER_ERROR';
  throw new TRPCError({
    code,
    message: result.error,
  });
}

/**
 * Get a typed DO stub for CloudAgentSession.
 */
function getSessionStub(
  env: { CLOUD_AGENT_SESSION: DurableObjectNamespace<CloudAgentSession> },
  doId: DurableObjectId
): DurableObjectStub<CloudAgentSession> {
  return env.CLOUD_AGENT_SESSION.get(doId);
}

/**
 * V2 session execution handlers.
 * These ask the DO to store pending delivery and flush through the shared wrapper path.
 */
export function createSessionExecutionV2Handlers() {
  return {
    /**
     * V2: Initialize from a prepared session.
     *
     * Uses a session created via prepareSession (for backend-to-backend flows).
     * The session must be in 'prepared' state (not yet initiated).
     * Returns an ack with delivery 'queued' after the DO stores pending delivery.
     * delivery 'sent' is reserved for idempotent replays already accepted by the wrapper.
     */
    initiateFromKilocodeSessionV2: protectedProcedure
      .input(InitiateFromPreparedSessionInput)
      .mutation(async ({ input, ctx }): Promise<QueueAckResponse> => {
        return withLogTags({ source: 'initiateFromKilocodeSessionV2' }, async () => {
          const sessionId = input.cloudAgentSessionId as SessionId;

          logger.setTags({
            userId: ctx.userId,
            sessionId,
            preparedSession: true,
          });

          logger.info('Initiating V2 session from prepared session');

          // Get DO stub
          const doKey = `${ctx.userId}:${sessionId}`;
          const doId = ctx.env.CLOUD_AGENT_SESSION.idFromName(doKey);

          const messageId = input.messageId ?? createMessageId();
          const startRequest: StartExecutionV2Request = {
            kind: 'initiatePrepared',
            userId: ctx.userId as `user_${string}`,
            botId: ctx.botId,
            authToken: ctx.authToken,
            messageId,
          };

          const startResult = await withDORetry<
            DurableObjectStub<CloudAgentSession>,
            StartExecutionV2Result
          >(
            () => getSessionStub(ctx.env, doId),
            stub => stub.startExecutionV2(startRequest),
            'startExecutionV2'
          );

          if (!startResult.success) {
            throwStartExecutionError(startResult);
          }

          logger.info(`V2 prepared session started: ${startResult.status}`);

          return {
            executionId: startResult.executionId,
            cloudAgentSessionId: sessionId,
            status: startResult.status,
            streamUrl: `/stream?cloudAgentSessionId=${sessionId}`,
            messageId: startResult.messageId,
            delivery: startResult.delivery,
          };
        });
      }),

    /**
     * V2: Send a message to an existing session.
     *
     * Sends a follow-up message to an established session.
     * Returns an ack with delivery 'queued' after the DO stores pending delivery.
     * delivery 'sent' is reserved for idempotent replays already accepted by the wrapper.
     */
    sendMessageV2: protectedProcedure
      .input(SendMessageV2Input)
      .mutation(async ({ input, ctx }): Promise<QueueAckResponse> => {
        return withLogTags({ source: 'sendMessageV2' }, async () => {
          const sessionId = input.cloudAgentSessionId as SessionId;

          logger.setTags({
            userId: ctx.userId,
            sessionId,
          });

          logger.info('Sending V2 message to existing session');

          // Get DO stub
          const doKey = `${ctx.userId}:${sessionId}`;
          const doId = ctx.env.CLOUD_AGENT_SESSION.idFromName(doKey);

          const messageId = input.messageId ?? createMessageId();
          const startRequest: StartExecutionV2Request = {
            kind: 'followup',
            userId: ctx.userId as `user_${string}`,
            botId: ctx.botId,
            prompt: input.prompt,
            mode: input.mode,
            model: input.model,
            variant: input.variant,
            autoCommit: input.autoCommit,
            condenseOnComplete: input.condenseOnComplete,
            messageId,
            images: input.images,
            tokenOverrides: {
              githubToken: input.githubToken,
              gitToken: input.gitToken,
            },
          };

          const startResult = await withDORetry<
            DurableObjectStub<CloudAgentSession>,
            StartExecutionV2Result
          >(
            () => getSessionStub(ctx.env, doId),
            stub => stub.startExecutionV2(startRequest),
            'startExecutionV2'
          );

          if (!startResult.success) {
            throwStartExecutionError(startResult);
          }

          logger.info(`V2 follow-up message started: ${startResult.status}`);

          return {
            executionId: startResult.executionId,
            cloudAgentSessionId: sessionId,
            status: startResult.status,
            streamUrl: `/stream?cloudAgentSessionId=${sessionId}`,
            messageId: startResult.messageId,
            delivery: startResult.delivery,
          };
        });
      }),
  };
}
