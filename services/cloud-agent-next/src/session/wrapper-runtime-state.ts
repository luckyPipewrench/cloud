import { z } from 'zod';

const WRAPPER_RUNTIME_STATE_KEY = 'wrapper_runtime_state';

const wrapperRuntimeStateSchema = z.object({
  wrapperGeneration: z.number().int().nonnegative(),
  wrapperConnectionId: z.string().optional(),
  wrapperExecutionId: z.string().optional(),
  lastWrapperConnectedAt: z.number().int().nonnegative().optional(),
  lastWrapperMessageAt: z.number().int().nonnegative().optional(),
  lastWrapperPongAt: z.number().int().nonnegative().optional(),
  pingDeadlineAt: z.number().int().nonnegative().optional(),
  nextPingAt: z.number().int().nonnegative().optional(),
  noOutputDeadlineAt: z.number().int().nonnegative().optional(),
  acceptedMessageId: z.string().optional(),
  acceptedExecutionId: z.string().optional(),
});

export type WrapperRuntimeState = z.infer<typeof wrapperRuntimeStateSchema>;

export const emptyWrapperRuntimeState = (): WrapperRuntimeState => ({
  wrapperGeneration: 0,
});

export async function getWrapperRuntimeState(
  storage: DurableObjectStorage
): Promise<WrapperRuntimeState> {
  const stored = await storage.get(WRAPPER_RUNTIME_STATE_KEY);
  const parsed = wrapperRuntimeStateSchema.safeParse(stored);
  return parsed.success ? parsed.data : emptyWrapperRuntimeState();
}

export async function allocateWrapperRuntimeState(
  storage: DurableObjectStorage,
  executionId: string,
  now = Date.now()
): Promise<WrapperRuntimeState> {
  const current = await getWrapperRuntimeState(storage);
  if (current.wrapperExecutionId === executionId && current.wrapperConnectionId) {
    const next = {
      wrapperGeneration: current.wrapperGeneration,
      wrapperConnectionId: current.wrapperConnectionId,
      wrapperExecutionId: current.wrapperExecutionId,
      lastWrapperConnectedAt: now,
    } satisfies WrapperRuntimeState;
    await storage.put(WRAPPER_RUNTIME_STATE_KEY, next);
    return next;
  }

  const next = {
    wrapperGeneration: current.wrapperGeneration + 1,
    wrapperConnectionId: crypto.randomUUID(),
    wrapperExecutionId: executionId,
    lastWrapperConnectedAt: now,
  } satisfies WrapperRuntimeState;
  await storage.put(WRAPPER_RUNTIME_STATE_KEY, next);
  return next;
}

export type WrapperRuntimeFence = {
  wrapperGeneration?: number;
  wrapperConnectionId?: string;
};

export async function clearWrapperRuntimeIdentityForExecution(
  storage: DurableObjectStorage,
  executionId: string,
  fence: WrapperRuntimeFence = {},
  opts: { incrementGeneration?: boolean } = {}
): Promise<WrapperRuntimeState | null> {
  const current = await getWrapperRuntimeState(storage);
  if (current.wrapperExecutionId !== executionId && current.acceptedExecutionId !== executionId) {
    return null;
  }
  if (
    fence.wrapperGeneration !== undefined &&
    current.wrapperGeneration !== fence.wrapperGeneration
  ) {
    return null;
  }
  if (
    fence.wrapperConnectionId !== undefined &&
    current.wrapperConnectionId !== fence.wrapperConnectionId
  ) {
    return null;
  }

  const next = {
    wrapperGeneration: opts.incrementGeneration
      ? current.wrapperGeneration + 1
      : current.wrapperGeneration,
  } satisfies WrapperRuntimeState;
  await storage.put(WRAPPER_RUNTIME_STATE_KEY, next);
  return next;
}

export async function clearAllocatedWrapperRuntimeState(
  storage: DurableObjectStorage,
  allocated: WrapperRuntimeState
): Promise<void> {
  if (!allocated.wrapperConnectionId || !allocated.wrapperExecutionId) return;

  await clearWrapperRuntimeIdentityForExecution(
    storage,
    allocated.wrapperExecutionId,
    {
      wrapperGeneration: allocated.wrapperGeneration,
      wrapperConnectionId: allocated.wrapperConnectionId,
    },
    { incrementGeneration: true }
  );
}

export async function isCurrentWrapperConnection(
  storage: DurableObjectStorage,
  wrapperGeneration: number,
  wrapperConnectionId: string
): Promise<boolean> {
  const current = await getWrapperRuntimeState(storage);
  return (
    current.wrapperGeneration === wrapperGeneration &&
    current.wrapperConnectionId === wrapperConnectionId
  );
}

async function updateIfCurrent(
  storage: DurableObjectStorage,
  wrapperGeneration: number,
  wrapperConnectionId: string,
  update: (current: WrapperRuntimeState) => WrapperRuntimeState
): Promise<WrapperRuntimeState | null> {
  const current = await getWrapperRuntimeState(storage);
  if (
    current.wrapperGeneration !== wrapperGeneration ||
    current.wrapperConnectionId !== wrapperConnectionId
  ) {
    return null;
  }

  const next = update(current);
  await storage.put(WRAPPER_RUNTIME_STATE_KEY, next);
  return next;
}

export async function recordWrapperAcceptedMessage(
  storage: DurableObjectStorage,
  allocated: WrapperRuntimeState,
  messageId: string,
  executionId: string,
  noOutputDeadlineAt: number,
  nextPingAt: number
): Promise<void> {
  if (!allocated.wrapperConnectionId) return;

  await updateIfCurrent(
    storage,
    allocated.wrapperGeneration,
    allocated.wrapperConnectionId,
    current => ({
      ...current,
      acceptedMessageId: messageId,
      acceptedExecutionId: executionId,
      noOutputDeadlineAt,
      nextPingAt:
        current.pingDeadlineAt === undefined ? (current.nextPingAt ?? nextPingAt) : undefined,
    })
  );
}

export async function recordWrapperPong(
  storage: DurableObjectStorage,
  wrapperGeneration: number,
  wrapperConnectionId: string,
  now = Date.now(),
  nextPingAt = now + 60_000
): Promise<WrapperRuntimeState | null> {
  return updateIfCurrent(storage, wrapperGeneration, wrapperConnectionId, current => ({
    ...current,
    lastWrapperPongAt: now,
    pingDeadlineAt: undefined,
    nextPingAt,
  }));
}

export async function recordMeaningfulWrapperOutput(
  storage: DurableObjectStorage,
  wrapperGeneration: number,
  wrapperConnectionId: string,
  now = Date.now(),
  nextPingAt = now + 60_000
): Promise<WrapperRuntimeState | null> {
  return updateIfCurrent(storage, wrapperGeneration, wrapperConnectionId, current => ({
    ...current,
    lastWrapperMessageAt: now,
    noOutputDeadlineAt: undefined,
    nextPingAt: current.pingDeadlineAt === undefined ? nextPingAt : undefined,
  }));
}

export async function markWrapperPingSent(
  storage: DurableObjectStorage,
  wrapperGeneration: number,
  wrapperConnectionId: string,
  pingDeadlineAt: number
): Promise<WrapperRuntimeState | null> {
  return updateIfCurrent(storage, wrapperGeneration, wrapperConnectionId, current => ({
    ...current,
    pingDeadlineAt,
    nextPingAt: undefined,
  }));
}

export async function clearCurrentWrapperRuntimeLivenessState(
  storage: DurableObjectStorage,
  wrapperGeneration: number,
  wrapperConnectionId: string
): Promise<WrapperRuntimeState | null> {
  return updateIfCurrent(storage, wrapperGeneration, wrapperConnectionId, current => ({
    wrapperGeneration: current.wrapperGeneration,
    wrapperConnectionId: current.wrapperConnectionId,
    wrapperExecutionId: current.wrapperExecutionId,
    lastWrapperConnectedAt: current.lastWrapperConnectedAt,
    lastWrapperMessageAt: current.lastWrapperMessageAt,
    lastWrapperPongAt: current.lastWrapperPongAt,
  }));
}

export async function clearCurrentWrapperRuntimeFailureState(
  storage: DurableObjectStorage,
  wrapperGeneration: number,
  wrapperConnectionId: string
): Promise<WrapperRuntimeState | null> {
  return updateIfCurrent(storage, wrapperGeneration, wrapperConnectionId, current => ({
    wrapperGeneration: current.wrapperGeneration + 1,
  }));
}
