/**
 * LinearManager - Linear ticket management service interface.
 *
 * Fetches assigned issues from the Linear GraphQL API, caches them,
 * and streams status updates to connected web clients.
 *
 * @module LinearManager
 */
import {
  type LinearAssignProjectInput,
  type LinearAssignProjectResult,
  type LinearError,
  type LinearSnapshot,
  type LinearStatusEvent,
} from "@t3tools/contracts";
import { Effect, ServiceMap, Stream } from "effect";

export interface LinearManagerShape {
  readonly list: () => Effect.Effect<LinearSnapshot, LinearError>;

  readonly refresh: () => Effect.Effect<LinearSnapshot, LinearError>;

  readonly assignProject: (
    input: LinearAssignProjectInput,
  ) => Effect.Effect<LinearAssignProjectResult, LinearError>;

  readonly streamStatus: Stream.Stream<LinearStatusEvent, LinearError>;
}

export class LinearManager extends ServiceMap.Service<LinearManager, LinearManagerShape>()(
  "lygos/linear/Services/LinearManager",
) {}
