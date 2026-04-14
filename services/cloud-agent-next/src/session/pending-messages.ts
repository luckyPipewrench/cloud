import * as z from 'zod';
import type { ExecutionMode } from '../execution/types.js';
import { AgentModeSchema } from '../schema.js';
import { MESSAGE_ID_FORMAT_DESCRIPTION, MESSAGE_ID_PATTERN } from './message-id.js';

export const PENDING_SESSION_MESSAGE_LIMIT = 10;
export const PENDING_FLUSH_RETRY_MAX_DELAY_MS = 15_000;
export const PENDING_FLUSH_RETRY_BASE_DELAY_MS = PENDING_FLUSH_RETRY_MAX_DELAY_MS;
export const PENDING_FLUSH_MAX_ATTEMPTS = 5;

const PENDING_MESSAGE_PREFIX = 'pending_message:';
const CREATED_AT_WIDTH = 16;

const PendingSessionMessageExecutionOptionsSchema = z.object({
  mode: AgentModeSchema.optional(),
  model: z.string().optional(),
  variant: z.string().optional(),
  autoCommit: z.boolean().optional(),
  condenseOnComplete: z.boolean().optional(),
  githubTokenOverride: z.string().optional(),
  gitTokenOverride: z.string().optional(),
});

export type PendingSessionMessageExecutionOptions = z.infer<
  typeof PendingSessionMessageExecutionOptionsSchema
>;

const PendingSessionExecutionKindSchema = z.enum(['initiate', 'initiatePrepared', 'followup']);

export type PendingSessionExecutionKind = z.infer<typeof PendingSessionExecutionKindSchema>;

export const PendingSessionMessageSchema = z.object({
  messageId: z.string().regex(MESSAGE_ID_PATTERN, MESSAGE_ID_FORMAT_DESCRIPTION),
  executionId: z.string().optional(),
  clientRequestId: z.string().optional(),
  role: z.literal('user'),
  content: z.string(),
  createdAt: z.number(),
  callbackUrl: z.string().optional(),
  callbackMetadata: z.unknown().optional(),
  executionKind: PendingSessionExecutionKindSchema.optional(),
  executionOptions: PendingSessionMessageExecutionOptionsSchema.optional(),
  flushAttempts: z.number().int().min(0).optional(),
  nextFlushAttemptAt: z.number().optional(),
  lastFlushError: z.string().optional(),
});

export type PendingSessionMessage = z.infer<typeof PendingSessionMessageSchema>;

export type PendingSessionMessageCapacity = {
  available: boolean;
  count: number;
  limit: number;
  message?: string;
};

export type PendingFlushFailureResult = {
  message: PendingSessionMessage;
  attempts: number;
  exhausted: boolean;
  nextFlushAttemptAt?: number;
};

export type PendingSessionExecutionDefaults = {
  mode?: ExecutionMode;
  model?: string;
  variant?: string;
  autoCommit?: boolean;
  condenseOnComplete?: boolean;
};

type PendingMessageEntry = {
  key: string;
  message: PendingSessionMessage;
};

export type SessionQueueStorage = {
  get<T = unknown>(key: string): Promise<T | undefined>;
  put(key: string, value: unknown): Promise<void>;
  delete(keys: string | string[]): Promise<unknown>;
  list<T = unknown>(options: { prefix: string }): Promise<Map<string, T>>;
};

function pendingMessageKey(
  message: Pick<PendingSessionMessage, 'createdAt' | 'messageId'>
): string {
  return `${PENDING_MESSAGE_PREFIX}${String(message.createdAt).padStart(CREATED_AT_WIDTH, '0')}:${message.messageId}`;
}

function hasExecutionOptions(
  executionOptions: PendingSessionMessageExecutionOptions | undefined
): executionOptions is PendingSessionMessageExecutionOptions {
  return Boolean(
    executionOptions &&
    (executionOptions.mode !== undefined ||
      executionOptions.model !== undefined ||
      executionOptions.variant !== undefined ||
      executionOptions.autoCommit !== undefined ||
      executionOptions.condenseOnComplete !== undefined ||
      executionOptions.githubTokenOverride !== undefined ||
      executionOptions.gitTokenOverride !== undefined)
  );
}

async function listPendingMessageEntries(
  storage: SessionQueueStorage
): Promise<PendingMessageEntry[]> {
  const entries = await storage.list<unknown>({ prefix: PENDING_MESSAGE_PREFIX });
  return Array.from(entries.entries()).flatMap(([key, value]) => {
    const result = PendingSessionMessageSchema.safeParse(value);
    return result.success ? [{ key, message: result.data }] : [];
  });
}

export function createPendingSessionMessage(params: {
  messageId: string;
  executionId?: string;
  clientRequestId?: string;
  role: 'user';
  content: string;
  createdAt: number;
  callbackUrl?: string;
  callbackMetadata?: unknown;
  executionKind?: PendingSessionExecutionKind;
  executionOptions?: PendingSessionMessageExecutionOptions;
}): PendingSessionMessage {
  const message = {
    ...params,
    executionOptions: hasExecutionOptions(params.executionOptions)
      ? params.executionOptions
      : undefined,
  } satisfies PendingSessionMessage;
  return PendingSessionMessageSchema.parse(message);
}

export function resolvePendingSessionMessageExecutionOptions(
  message: PendingSessionMessage,
  defaults: PendingSessionExecutionDefaults
): {
  mode?: ExecutionMode;
  model?: string;
  variant?: string;
  autoCommit?: boolean;
  condenseOnComplete?: boolean;
  githubTokenOverride?: string;
  gitTokenOverride?: string;
} {
  return {
    mode: message.executionOptions?.mode ?? defaults.mode,
    model: message.executionOptions?.model ?? defaults.model,
    variant: message.executionOptions?.variant ?? defaults.variant,
    autoCommit: message.executionOptions?.autoCommit ?? defaults.autoCommit,
    condenseOnComplete: message.executionOptions?.condenseOnComplete ?? defaults.condenseOnComplete,
    githubTokenOverride: message.executionOptions?.githubTokenOverride,
    gitTokenOverride: message.executionOptions?.gitTokenOverride,
  };
}

export async function storePendingSessionMessage(
  storage: SessionQueueStorage,
  message: PendingSessionMessage
): Promise<void> {
  await storage.put(pendingMessageKey(message), PendingSessionMessageSchema.parse(message));
}

export async function listPendingSessionMessages(
  storage: SessionQueueStorage
): Promise<PendingSessionMessage[]> {
  const entries = await listPendingMessageEntries(storage);
  return entries.map(entry => entry.message);
}

export async function countPendingSessionMessages(storage: SessionQueueStorage): Promise<number> {
  const entries = await listPendingMessageEntries(storage);
  return entries.length;
}

export async function clearPendingSessionMessages(
  storage: SessionQueueStorage
): Promise<PendingSessionMessage[]> {
  const entries = await listPendingMessageEntries(storage);
  if (entries.length === 0) return [];

  await storage.delete(entries.map(entry => entry.key));
  return entries.map(entry => entry.message);
}

export async function checkPendingSessionMessageCapacity(
  storage: SessionQueueStorage
): Promise<PendingSessionMessageCapacity> {
  const count = await countPendingSessionMessages(storage);
  const available = count < PENDING_SESSION_MESSAGE_LIMIT;
  return {
    available,
    count,
    limit: PENDING_SESSION_MESSAGE_LIMIT,
    message: available
      ? undefined
      : `Pending message queue is full (${PENDING_SESSION_MESSAGE_LIMIT})`,
  };
}

export function shouldSkipPendingFlush(message: PendingSessionMessage, now: number): boolean {
  return message.nextFlushAttemptAt !== undefined && message.nextFlushAttemptAt > now;
}

export async function recordPendingFlushFailure(
  storage: SessionQueueStorage,
  message: PendingSessionMessage,
  error: string,
  now: number
): Promise<PendingFlushFailureResult> {
  const attempts = (message.flushAttempts ?? 0) + 1;
  const exhausted = attempts >= PENDING_FLUSH_MAX_ATTEMPTS;
  const nextFlushAttemptAt = exhausted
    ? undefined
    : now +
      Math.min(
        PENDING_FLUSH_RETRY_BASE_DELAY_MS * 2 ** (attempts - 1),
        PENDING_FLUSH_RETRY_MAX_DELAY_MS
      );
  const updated: PendingSessionMessage = {
    ...message,
    flushAttempts: attempts,
    nextFlushAttemptAt,
    lastFlushError: error,
  };

  if (exhausted) {
    await deletePendingSessionMessageByMessageId(storage, message.messageId);
  } else {
    await deletePendingSessionMessageByMessageId(storage, message.messageId);
    await storePendingSessionMessage(storage, updated);
  }

  return { message: updated, attempts, exhausted, nextFlushAttemptAt };
}

export async function deletePendingSessionMessageByMessageId(
  storage: SessionQueueStorage,
  messageId: string
): Promise<boolean> {
  const entries = await listPendingMessageEntries(storage);
  const matchingEntries = entries.filter(candidate => candidate.message.messageId === messageId);
  if (matchingEntries.length === 0) return false;

  await storage.delete(matchingEntries.map(entry => entry.key));
  return true;
}

export async function findPendingSessionMessageByMessageId(
  storage: SessionQueueStorage,
  messageId: string
): Promise<PendingSessionMessage | undefined> {
  const entries = await listPendingMessageEntries(storage);
  return entries.find(entry => entry.message.messageId === messageId)?.message;
}

export async function findPendingSessionMessageByClientRequestId(
  storage: SessionQueueStorage,
  clientRequestId: string
): Promise<PendingSessionMessage | undefined> {
  const entries = await listPendingMessageEntries(storage);
  return entries.find(entry => entry.message.clientRequestId === clientRequestId)?.message;
}
