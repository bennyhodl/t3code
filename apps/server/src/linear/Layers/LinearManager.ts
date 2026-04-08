/**
 * LinearManagerLive - Layer implementation for Linear ticket management.
 *
 * Polls the Linear GraphQL API for assigned issues, caches results,
 * and streams snapshots to connected web clients. Reacts to settings
 * changes (token added/removed/changed) to start/stop polling.
 *
 * @module LinearManagerLive
 */
import {
  type LinearAssignProjectInput,
  type LinearAssignProjectResult,
  type LinearIssue,
  LinearNotConfiguredError,
  type LinearProject,
  type LinearSnapshot,
  type LinearStatusEvent,
} from "@t3tools/contracts";
import { Effect, Layer, Queue, Stream, SynchronizedRef } from "effect";

import { ServerSettingsService } from "../../serverSettings";
import { LinearManager, type LinearManagerShape } from "../Services/LinearManager";
import { assignIssueProject, fetchAssignedIssues, fetchProjects } from "./LinearGraphQLClient";

const POLL_INTERVAL_MS = 60_000;

interface LinearState {
  issues: LinearIssue[];
  projects: LinearProject[];
  connected: boolean;
}

function buildSnapshot(state: LinearState): LinearSnapshot {
  return {
    issues: state.issues,
    projects: state.projects,
    connected: state.connected,
  };
}

const EMPTY_STATE: LinearState = { issues: [], projects: [], connected: false };

const makeLinearManager = Effect.fn("makeLinearManager")(function* () {
  const serverSettings = yield* ServerSettingsService;
  const services = yield* Effect.services();
  const runFork = Effect.runForkWith(services);

  const stateRef = yield* SynchronizedRef.make<LinearState>(EMPTY_STATE);

  // ── Listeners ─────────────────────────────────────────────────────────

  const statusListeners = new Set<(event: LinearStatusEvent) => void>();

  const broadcastStatus = () => {
    const state = Effect.runSyncWith(services)(SynchronizedRef.get(stateRef));
    const snapshot = buildSnapshot(state);
    for (const listener of statusListeners) {
      listener(snapshot);
    }
  };

  // ── Polling ───────────────────────────────────────────────────────────

  let pollInterval: ReturnType<typeof setInterval> | null = null;

  const fetchAndUpdate = async (token: string): Promise<void> => {
    try {
      const [issues, projects] = await Effect.runPromise(
        Effect.all([fetchAssignedIssues(token), fetchProjects(token)]),
      );
      Effect.runSyncWith(services)(
        SynchronizedRef.set(stateRef, { issues, projects, connected: true }),
      );
      broadcastStatus();
    } catch (err) {
      console.error("[linear] Poll failed:", err);
    }
  };

  const startPolling = (token: string) => {
    stopPolling();
    // Fetch immediately, then poll
    void fetchAndUpdate(token);
    pollInterval = setInterval(() => void fetchAndUpdate(token), POLL_INTERVAL_MS);
  };

  const stopPolling = () => {
    if (pollInterval !== null) {
      clearInterval(pollInterval);
      pollInterval = null;
    }
  };

  // ── React to settings changes ──────────────────────────────────────────

  const settings = yield* serverSettings.getSettings;
  let currentToken = settings.linear.apiToken;

  if (currentToken) {
    startPolling(currentToken);
  }

  // Watch for token changes in background
  runFork(
    Effect.gen(function* () {
      yield* serverSettings.streamChanges.pipe(
        Stream.runForEach((newSettings) =>
          Effect.sync(() => {
            const newToken = newSettings.linear.apiToken;
            if (newToken === currentToken) return;

            currentToken = newToken;
            if (newToken) {
              startPolling(newToken);
            } else {
              stopPolling();
              Effect.runSyncWith(services)(SynchronizedRef.set(stateRef, EMPTY_STATE));
              broadcastStatus();
            }
          }),
        ),
      );
    }),
  );

  // ── Cleanup ────────────────────────────────────────────────────────────

  yield* Effect.addFinalizer(() =>
    Effect.sync(() => {
      stopPolling();
    }),
  );

  // ── Service shape ──────────────────────────────────────────────────────

  return {
    list: () =>
      Effect.gen(function* () {
        if (!currentToken) {
          return { issues: [], projects: [], connected: false };
        }
        const state = yield* SynchronizedRef.get(stateRef);
        return buildSnapshot(state);
      }),

    refresh: () =>
      Effect.gen(function* () {
        if (!currentToken) {
          return { issues: [], projects: [], connected: false };
        }
        const [issues, projects] = yield* Effect.all([
          fetchAssignedIssues(currentToken),
          fetchProjects(currentToken),
        ]);
        yield* SynchronizedRef.set(stateRef, { issues, projects, connected: true });
        broadcastStatus();
        // Restart polling timer so next poll is a full interval from now
        startPolling(currentToken);
        return buildSnapshot({ issues, projects, connected: true });
      }),

    assignProject: (input: LinearAssignProjectInput) =>
      Effect.gen(function* () {
        if (!currentToken) {
          return yield* new LinearNotConfiguredError({
            detail: "Linear API token not configured",
          });
        }

        const result: LinearAssignProjectResult = yield* assignIssueProject(
          currentToken,
          input.issueId,
          input.projectId,
        );

        // Update local cache: update the issue's project
        yield* SynchronizedRef.update(stateRef, (state) => ({
          ...state,
          issues: state.issues.map((issue) =>
            issue.id === input.issueId ? { ...issue, project: result.project } : issue,
          ),
        }));
        broadcastStatus();

        return result;
      }),

    streamStatus: Stream.callback<LinearStatusEvent>((queue) =>
      Effect.acquireRelease(
        Effect.sync(() => {
          // Send initial snapshot
          const state = Effect.runSyncWith(services)(SynchronizedRef.get(stateRef));
          Effect.runSyncWith(services)(Queue.offer(queue, buildSnapshot(state)));

          // Subscribe to future updates
          const listener = (event: LinearStatusEvent) => {
            Effect.runSyncWith(services)(Queue.offer(queue, event));
          };
          statusListeners.add(listener);
          return listener;
        }),
        (listener) =>
          Effect.sync(() => {
            statusListeners.delete(listener);
          }),
      ),
    ),
  } satisfies LinearManagerShape;
});

export const LinearManagerLive = Layer.effect(LinearManager, makeLinearManager());
