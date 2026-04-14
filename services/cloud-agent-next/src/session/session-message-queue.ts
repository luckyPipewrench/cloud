import type { ExecutionMode, ExecutionPlan, StartExecutionV2Result } from '../execution/types.js';
import type { ExecutionId, SessionId, UserId } from '../types/ids.js';
import {
  createPendingSessionMessage,
  listPendingSessionMessages,
  findPendingSessionMessageByMessageId,
  deletePendingSessionMessageByMessageId,
  recordPendingFlushFailure,
  resolvePendingSessionMessageExecutionOptions,
  shouldSkipPendingFlush,
  storePendingSessionMessage,
  type PendingSessionExecutionDefaults,
  type PendingSessionExecutionKind,
  type PendingSessionMessage,
  type PendingSessionMessageExecutionOptions,
  type SessionQueueStorage,
} from './pending-messages.js';

export type QueueExecutionPlanParams = {
  executionId: ExecutionId;
  messageId: string;
  prompt: string;
  mode: ExecutionMode;
  wrapper: ExecutionPlan['wrapper'];
  workspace: ExecutionPlan['workspace'];
  executionKind?: PendingSessionExecutionKind;
};

function inferExecutionKindFromPlan(
  plan: Pick<QueueExecutionPlanParams, 'workspace' | 'executionKind'>
): PendingSessionExecutionKind {
  return plan.executionKind ?? (plan.workspace.shouldPrepare ? 'initiate' : 'followup');
}

export function createPendingSessionMessageFromPlan(
  plan: QueueExecutionPlanParams,
  createdAt = Date.now()
): PendingSessionMessage {
  const executionOptions: PendingSessionMessageExecutionOptions = {
    mode: plan.mode,
    model: plan.wrapper.model?.modelID,
    variant: plan.wrapper.variant,
    autoCommit: plan.wrapper.autoCommit,
    condenseOnComplete: plan.wrapper.condenseOnComplete,
    githubTokenOverride: plan.workspace.shouldPrepare
      ? undefined
      : plan.workspace.resumeContext.githubToken,
    gitTokenOverride: plan.workspace.shouldPrepare
      ? undefined
      : plan.workspace.resumeContext.gitToken,
  };

  return createPendingSessionMessage({
    messageId: plan.messageId,
    executionId: plan.executionId,
    role: 'user',
    content: plan.prompt,
    createdAt,
    executionKind: inferExecutionKindFromPlan(plan),
    executionOptions,
  });
}

export type PendingFlushPlanContext = {
  sessionId: SessionId;
  userId: UserId;
  orgId?: string;
  sandboxId: string;
  kiloSessionId?: string;
  metadata: {
    initiatedAt?: number;
    initialMessageId?: string;
    prompt?: string;
    mode?: string;
    model?: string;
    variant?: string;
    autoCommit?: boolean;
    condenseOnComplete?: boolean;
    kilocodeToken?: string;
    githubToken?: string;
  };
};

export type PendingFlushFailure = {
  type: 'failure';
  message: PendingSessionMessage;
  attempts: number;
  exhausted: boolean;
  nextFlushAttemptAt?: number;
};

export type PendingFlushSkipped = {
  type: 'skipped';
  nextFlushAttemptAt?: number;
};

export type PendingFlushDelivered = {
  type: 'delivered';
};

export type PendingFlushResult = PendingFlushFailure | PendingFlushSkipped | PendingFlushDelivered;

function resolvePendingExecutionKind(
  message: PendingSessionMessage,
  context: PendingFlushPlanContext
): PendingSessionExecutionKind {
  if (message.executionKind) {
    return message.executionKind;
  }

  if (
    context.metadata.initialMessageId !== undefined &&
    message.messageId === context.metadata.initialMessageId
  ) {
    return 'initiatePrepared';
  }

  if (
    !context.metadata.initiatedAt &&
    context.metadata.prompt !== undefined &&
    message.content === context.metadata.prompt
  ) {
    return 'initiatePrepared';
  }

  return 'followup';
}

export async function flushNextPendingSessionMessage(params: {
  storage: SessionQueueStorage;
  now: number;
  hasCurrentRuntimeExecution: () => Promise<boolean>;
  getMetadataContext: () => Promise<PendingFlushPlanContext | null>;
  buildPlan: (args: {
    message: PendingSessionMessage;
    executionId: ExecutionId;
    executionKind: PendingSessionExecutionKind;
    options: ReturnType<typeof resolvePendingSessionMessageExecutionOptions>;
    context: PendingFlushPlanContext;
  }) => Promise<ExecutionPlan>;
  deliver: (plan: ExecutionPlan, message: PendingSessionMessage) => Promise<StartExecutionV2Result>;
  onInferredExecutionKind?: (
    message: PendingSessionMessage,
    executionKind: PendingSessionExecutionKind
  ) => void;
}): Promise<PendingFlushResult> {
  const [message] = await listPendingSessionMessages(params.storage);

  if (!message) {
    return { type: 'skipped' };
  }

  if (shouldSkipPendingFlush(message, params.now)) {
    return { type: 'skipped', nextFlushAttemptAt: message.nextFlushAttemptAt };
  }

  if (await params.hasCurrentRuntimeExecution()) {
    return { type: 'skipped' };
  }

  const context = await params.getMetadataContext();
  if (!context) {
    const failure = await recordPendingFlushFailure(
      params.storage,
      message,
      'Session metadata is not available',
      params.now
    );
    return { type: 'failure', ...failure };
  }

  const executionKind = resolvePendingExecutionKind(message, context);
  if (!message.executionKind) {
    params.onInferredExecutionKind?.(message, executionKind);
  }

  if (!context.metadata.initiatedAt && executionKind === 'followup') {
    const failure = await recordPendingFlushFailure(
      params.storage,
      message,
      'Session has not been initiated',
      params.now
    );
    return { type: 'failure', ...failure };
  }

  const options = resolvePendingSessionMessageExecutionOptions(message, {
    mode: context.metadata.mode as ExecutionMode | undefined,
    model: context.metadata.model,
    variant: context.metadata.variant,
    autoCommit: context.metadata.autoCommit,
    condenseOnComplete: context.metadata.condenseOnComplete,
  } satisfies PendingSessionExecutionDefaults);

  if (!options.model) {
    const failure = await recordPendingFlushFailure(
      params.storage,
      message,
      'Session is missing a valid model',
      params.now
    );
    return { type: 'failure', ...failure };
  }

  const executionId = message.executionId as ExecutionId;

  try {
    const plan = await params.buildPlan({ message, executionId, executionKind, options, context });
    const startResult = await params.deliver(plan, message);
    if (!startResult.success) {
      throw new Error(startResult.error);
    }
    await deletePendingSessionMessageByMessageId(params.storage, message.messageId);
    return { type: 'delivered' };
  } catch (error) {
    const failure = await recordPendingFlushFailure(
      params.storage,
      message,
      error instanceof Error ? error.message : String(error),
      params.now
    );
    return { type: 'failure', ...failure };
  }
}

export async function enqueuePendingSessionMessage(
  storage: SessionQueueStorage,
  plan: QueueExecutionPlanParams,
  createdAt = Date.now(),
  executionKind?: PendingSessionExecutionKind
): Promise<PendingSessionMessage> {
  const message = createPendingSessionMessageFromPlan(
    { ...plan, executionKind: executionKind ?? plan.executionKind },
    createdAt
  );
  await storePendingSessionMessage(storage, message);
  return message;
}

export async function getQueuedMessageByMessageId(
  storage: SessionQueueStorage,
  messageId: string
): Promise<PendingSessionMessage | undefined> {
  return findPendingSessionMessageByMessageId(storage, messageId);
}
