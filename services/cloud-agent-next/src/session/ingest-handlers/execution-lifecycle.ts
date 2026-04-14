export type ExecutionStatus = 'completed' | 'failed' | 'interrupted';

export type ExecutionLifecycleContext = {
  updateExecutionStatus: (
    executionId: string,
    status: ExecutionStatus,
    error?: string,
    gateResult?: 'pass' | 'fail'
  ) => Promise<void>;
  logger: { info: (msg: string, data?: object) => void };
};

/**
 * Handle execution completion by updating terminal status.
 */
export async function handleExecutionComplete(
  executionId: string,
  status: ExecutionStatus,
  ctx: ExecutionLifecycleContext,
  error?: string,
  gateResult?: 'pass' | 'fail'
): Promise<void> {
  ctx.logger.info('Execution complete', { executionId, status, error, gateResult });

  // Update the execution status and completedAt in storage
  await ctx.updateExecutionStatus(executionId, status, error, gateResult);
}
