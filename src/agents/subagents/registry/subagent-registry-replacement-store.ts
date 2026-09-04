import { isDeepStrictEqual } from "node:util";
import { runOpenClawStateWriteTransaction } from "../../../state/openclaw-state-db.js";
import { publishTaskRecordAfterAtomicStore } from "../../../tasks/runtime-internal.js";
import type { PreparedCanonicalTaskActivation } from "../../../tasks/task-backing-authority-write.js";
import { readTaskBackingInstance } from "../../../tasks/task-backing-authority.js";
import {
  bindTaskRecord,
  readTaskRecord,
  upsertTaskRunRowInDatabase,
} from "../../../tasks/task-registry.store.sqlite.js";
import type { TaskRecord } from "../../../tasks/task-registry.types.js";
import { publishSubagentRunsAfterAtomicStore } from "./subagent-registry-state.js";
import {
  bindSubagentRunRecord,
  deleteSubagentRunRowInDatabase,
  readSubagentRun,
  upsertSubagentRunRowInDatabase,
} from "./subagent-registry.store.sqlite.js";
import type { SubagentRunRecord } from "./subagent-registry.types.js";

function assertReplacementCorrelation(params: {
  source: SubagentRunRecord;
  successor: SubagentRunRecord;
  task: PreparedCanonicalTaskActivation;
}): void {
  const sourceBacking = readTaskBackingInstance(params.task.current.detail);
  const successorBacking = readTaskBackingInstance(params.task.next.detail);
  const canonicalRunId = params.source.taskRunId ?? params.source.runId;
  if (
    sourceBacking?.runtime !== "subagent" ||
    successorBacking?.runtime !== "subagent" ||
    sourceBacking.generation !== params.source.generation ||
    successorBacking.generation !== params.successor.generation ||
    params.task.current.runtime !== "subagent" ||
    params.task.current.runId !== canonicalRunId ||
    params.task.current.childSessionKey !== params.source.childSessionKey ||
    params.successor.taskRunId !== params.source.taskRunId ||
    params.successor.childSessionKey !== params.source.childSessionKey ||
    params.task.next.status !== "running"
  ) {
    throw new Error("replacement subagent and task do not share one owner generation");
  }
}

/** Atomically transfers one subagent owner generation and reactivates its canonical task. */
export function commitSubagentTaskReplacement(params: {
  runs: Map<string, SubagentRunRecord>;
  changedRunIds: readonly string[];
  source: SubagentRunRecord;
  successor: SubagentRunRecord;
  task: PreparedCanonicalTaskActivation;
}): TaskRecord {
  assertReplacementCorrelation(params);
  const changedRows = params.changedRunIds.flatMap((runId) => {
    const entry = params.runs.get(runId);
    return entry ? [bindSubagentRunRecord(entry)] : [];
  });
  const deletedRunIds = params.changedRunIds.filter((runId) => !params.runs.has(runId));
  const sourceRow = bindSubagentRunRecord(params.source);
  const currentTaskRow = bindTaskRecord(params.task.current);
  const taskRow = bindTaskRecord(params.task.next);

  runOpenClawStateWriteTransaction(
    (database) => {
      const storedSource = readSubagentRun(database, params.source.runId);
      const storedTask = readTaskRecord(database.db, params.task.current.taskId);
      if (!storedSource || !isDeepStrictEqual(bindSubagentRunRecord(storedSource), sourceRow)) {
        throw new Error("replacement subagent source changed before commit");
      }
      if (!storedTask || !isDeepStrictEqual(bindTaskRecord(storedTask), currentTaskRow)) {
        throw new Error("replacement task source changed before commit");
      }
      for (const row of changedRows) {
        upsertSubagentRunRowInDatabase(database, row);
      }
      for (const runId of deletedRunIds) {
        deleteSubagentRunRowInDatabase(database, runId);
      }
      upsertTaskRunRowInDatabase(database, taskRow);
    },
    undefined,
    { operationLabel: "subagent task replacement" },
  );
  publishSubagentRunsAfterAtomicStore(params.runs, params.changedRunIds);
  return publishTaskRecordAfterAtomicStore(params.task.next);
}
