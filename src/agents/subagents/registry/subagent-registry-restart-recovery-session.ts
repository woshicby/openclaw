import { getRuntimeConfig } from "../../../config/config.js";
import {
  resolveAgentIdFromSessionKey,
  resolveSessionStorePathCore,
} from "../../../config/sessions.js";
import {
  loadSessionEntry,
  patchSessionEntryCore,
} from "../../../config/sessions/session-accessor.js";
import type { InternalSessionEntry } from "../../../config/sessions/types.js";
import type { GatewayRecoveryRuntime } from "../../../gateway/server-instance-runtime.types.js";
import { listAgentRunsForSession } from "../../../infra/agent-run-registry.js";
import { isSessionWorkAdmissionActive } from "../../../sessions/session-lifecycle-admission.js";
import { isRetiredRunningSubagent } from "./subagent-registry-restart-recovery-helpers.js";
import type {
  SubagentRestartRecoveryReceipt,
  SubagentRunRecord,
} from "./subagent-registry.types.js";

const RECOVERY_RESUMED_NOTICE = "Resumed your interrupted task after the Gateway restart.";

export async function loadSubagentRecoverySession(params: {
  entry: SubagentRunRecord;
  isOwnerCurrent: () => boolean;
  now: number;
}): Promise<{
  agentId: string;
  storePath: string;
  sessionEntry: InternalSessionEntry | undefined;
} | null> {
  const sessionKey = params.entry.childSessionKey.trim();
  const agentId = resolveAgentIdFromSessionKey(sessionKey);
  const storePath = resolveSessionStorePathCore(getRuntimeConfig().session?.store, { agentId });
  const sessionEntry = loadSessionEntry({ storePath, sessionKey, clone: false });
  if (
    params.entry.execution.restartRecovery ||
    sessionEntry?.abortedLastRun === true ||
    sessionEntry?.status !== "running" ||
    sessionEntry.lifecycleRunId !== params.entry.runId ||
    !isRetiredRunningSubagent(params.entry)
  ) {
    return { agentId, storePath, sessionEntry };
  }
  const { sessionId, lifecycleRevision, updatedAt } = sessionEntry;
  const target = { sessionKey, sessionId };
  const isCurrent = () =>
    params.isOwnerCurrent() &&
    isRetiredRunningSubagent(params.entry) &&
    listAgentRunsForSession(target).length === 0 &&
    !isSessionWorkAdmissionActive(storePath, [sessionKey, sessionId]);
  const interrupted = await patchSessionEntryCore(
    { storePath, sessionKey },
    (current) => {
      if (
        !isCurrent() ||
        current.sessionId !== sessionId ||
        current.lifecycleRevision !== lifecycleRevision ||
        current.updatedAt !== updatedAt ||
        current.status !== "running" ||
        current.lifecycleRunId !== params.entry.runId
      ) {
        return null;
      }
      // A hard kill cannot write the shutdown marker. Bind its replacement to
      // this exact retired run, never a newer turn sharing the child session.
      return { ...current, abortedLastRun: true, updatedAt: params.now };
    },
    {
      assertCommitAllowed: () => {
        if (!isCurrent()) {
          throw new Error("subagent orphan ownership changed before interruption commit");
        }
      },
      replaceEntry: true,
      skipMaintenance: true,
    },
  );
  return interrupted ? { agentId, storePath, sessionEntry: interrupted } : null;
}

export async function confirmAcceptedRecoveryResumption(params: {
  childSessionKey: string;
  gatewayRuntime: GatewayRecoveryRuntime | undefined;
  owner: SubagentRunRecord;
  receipt: SubagentRestartRecoveryReceipt;
  warn: (message: string, meta: Record<string, unknown>) => void;
}): Promise<void> {
  const origin = params.owner.requesterOrigin;
  if (!origin?.channel || !origin.to) {
    return;
  }
  if (!params.gatewayRuntime) {
    return;
  }
  try {
    await params.gatewayRuntime.sendRecoveryNotice({
      channel: origin.channel,
      to: origin.to,
      accountId: origin.accountId,
      threadId: origin.threadId,
      text: RECOVERY_RESUMED_NOTICE,
      idempotencyKey: `${params.receipt.idempotencyKey}:resumed-notice`,
    });
  } catch (error) {
    params.warn("accepted subagent restart recovery could not confirm resumption", {
      runId: params.owner.runId,
      childSessionKey: params.childSessionKey,
      error,
    });
  }
}

export async function settleAcceptedRecoverySession(params: {
  attempts: number;
  childSessionKey: string;
  isOwnerCurrent: () => boolean;
  sessionId: string;
  sessionLifecycleRevision?: string;
  now: number;
  runId: string;
  storePath: string;
}): Promise<boolean> {
  let settled = false;
  await patchSessionEntryCore(
    { storePath: params.storePath, sessionKey: params.childSessionKey },
    (current) => {
      if (
        !params.isOwnerCurrent() ||
        current.sessionId !== params.sessionId ||
        (params.sessionLifecycleRevision !== undefined &&
          current.lifecycleRevision !== params.sessionLifecycleRevision)
      ) {
        return current;
      }
      if (current.abortedLastRun !== true) {
        settled = true;
        return current;
      }
      current.abortedLastRun = false;
      current.subagentRecovery = {
        automaticAttempts: Math.max(
          current.subagentRecovery?.automaticAttempts ?? 0,
          params.attempts + 1,
        ),
        lastAttemptAt: params.now,
        lastRunId: params.runId,
      };
      current.updatedAt = params.now;
      settled = true;
      return current;
    },
    {
      assertCommitAllowed: () => {
        if (!params.isOwnerCurrent()) {
          throw new Error("subagent restart recovery lifecycle retired before session commit");
        }
      },
      replaceEntry: true,
      skipMaintenance: true,
    },
  );
  return settled;
}
