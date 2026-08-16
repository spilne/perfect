// ---------------------------------------------------------------------------
// Branded topology identifiers.
//
// ConsumerGroup / ChannelName / CheckpointName come from @perfect/core/connect
// (the contracts topology consumes are defined there); StageId is
// topology-local. StagePlan carries id / repartitionTopic / group side by
// side — all strings, all confusable without brands.
// ---------------------------------------------------------------------------

import { type Brand, nominal } from "@perfect/core";

export { ConsumerGroup, ChannelName, CheckpointName } from "@perfect/core/connect";

/** Identifier of a planned topology stage (derived from group + index). */
export type StageId = Brand<string, "StageId">;
// Signature pinned explicitly (runtime-free cast) so the export doesn't
// reference core's unexported BRAND symbol (TS4023).
export const StageId = nominal<StageId>() as (value: string) => StageId;
