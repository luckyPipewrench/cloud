import { describe, expect, it } from 'vitest';
import { PreparationInputSchema } from '../persistence/schemas.js';
import {
  ExecutionResponse,
  GetSessionOutput,
  InitiateFromPreparedSessionInput,
  SendMessageV2Input,
} from './schemas.js';

const validMessageId = 'msg_018f1e2d3c4bAbCdEfGhIjKlMn';
const validSessionId = 'agent_12345678-1234-1234-1234-123456789012';
const basePromptInput = {
  prompt: 'continue',
  mode: 'code' as const,
  model: 'claude-sonnet-4-5-20250929',
  variant: 'thinking',
};
const baseSendMessageInput = {
  cloudAgentSessionId: validSessionId,
  ...basePromptInput,
};
const basePreparationInput = {
  sessionId: validSessionId,
  userId: 'user_test',
  authToken: 'token_test',
  ...basePromptInput,
  autoInitiate: false,
};

describe('message ID schema validation', () => {
  it('accepts canonical message IDs on public schemas', () => {
    expect(
      SendMessageV2Input.safeParse({ ...baseSendMessageInput, messageId: validMessageId }).success
    ).toBe(true);
    expect(SendMessageV2Input.safeParse({ ...baseSendMessageInput, messageId: null }).success).toBe(
      true
    );
    expect(
      InitiateFromPreparedSessionInput.safeParse({
        cloudAgentSessionId: validSessionId,
        messageId: validMessageId,
      }).success
    ).toBe(true);
    expect(
      GetSessionOutput.safeParse({
        sessionId: validSessionId,
        userId: 'user_test',
        execution: null,
        initialMessageId: validMessageId,
        timestamp: Date.now(),
        version: 1,
      }).success
    ).toBe(true);
    expect(
      ExecutionResponse.safeParse({
        cloudAgentSessionId: validSessionId,
        executionId: 'exc_01KNSZD4EW94YSDE0WTTJYEQYH',
        status: 'started',
        streamUrl: 'https://example.com/stream',
        messageId: validMessageId,
        delivery: 'sent',
      }).success
    ).toBe(true);
    expect(
      PreparationInputSchema.safeParse({
        ...basePreparationInput,
        initialMessageId: validMessageId,
      }).success
    ).toBe(true);
  });

  it('rejects non-canonical message IDs on public schemas', () => {
    const invalidMessageIds = [
      'msg_018F1e2d3c4bAbCdEfGhIjKlMn',
      'msg_018f1e2d3c4bAbCdEfGhIjKlM-',
      'msg_018f1e2d3c4bAbCdEfGhIjKlM',
    ];

    for (const messageId of invalidMessageIds) {
      expect(SendMessageV2Input.safeParse({ ...baseSendMessageInput, messageId }).success).toBe(
        false
      );
      expect(
        InitiateFromPreparedSessionInput.safeParse({
          cloudAgentSessionId: validSessionId,
          messageId,
        }).success
      ).toBe(false);
      expect(
        GetSessionOutput.safeParse({
          sessionId: validSessionId,
          userId: 'user_test',
          execution: null,
          initialMessageId: messageId,
          timestamp: Date.now(),
          version: 1,
        }).success
      ).toBe(false);
      expect(
        ExecutionResponse.safeParse({
          cloudAgentSessionId: validSessionId,
          executionId: 'exc_01KNSZD4EW94YSDE0WTTJYEQYH',
          status: 'started',
          streamUrl: 'https://example.com/stream',
          messageId,
          delivery: 'sent',
        }).success
      ).toBe(false);
      expect(
        PreparationInputSchema.safeParse({
          ...basePreparationInput,
          initialMessageId: messageId,
        }).success
      ).toBe(false);
    }
  });
});
