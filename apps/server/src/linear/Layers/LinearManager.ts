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
  type LinearAssignLabelInput,
  type LinearAssignLabelResult,
  type LinearIssue,
  type LinearLabel,
  LinearNotConfiguredError,
  type LinearSnapshot,
  type LinearStatusEvent,
} from "@t3tools/contracts";
import {
  Effect,
  Exit,
  Fiber,
  Layer,
  Queue,
  Schedule,
  Scope,
  Stream,
  SynchronizedRef,
} from "effect";
import * as HttpClient from "effect/unstable/http/HttpClient";

import { ServerSettingsService } from "../../serverSettings.ts";
import { LinearManager, type LinearManagerShape } from "../Services/LinearManager.ts";
import { assignIssueLabel, fetchAssignedIssues, fetchLabels } from "./LinearGraphQLClient.ts";

const POLL_INTERVAL = "60 seconds";

interface LinearState {
  issues: LinearIssue[];
  labels: LinearLabel[];
  connected: boolean;
}

function buildSnapshot(state: LinearState): LinearSnapshot {
  return {
    issues: state.issues,
    labels: state.labels,
    connected: state.connected,
  };
}

const EMPTY_STATE: LinearState = { issues: [], labels: [], connected: false };

const makeLinearManager = Effect.fn("makeLinearManager")(function* () {
  const serverSettings = yield* ServerSettingsService;
  const httpClient = yield* HttpClient.HttpClient;
  const context = yield* Effect.context<never>();
  const runFork = Effect.runForkWith(context);

  // The poll loop is owned by a scope of its own so restarting it never needs
  // Scope in the caller's requirements.
  const pollScope = yield* Scope.make("sequential");
  yield* Effect.addFinalizer(() => Scope.close(pollScope, Exit.void));

  const stateRef = yield* SynchronizedRef.make<LinearState>(EMPTY_STATE);

  // ── Listeners ─────────────────────────────────────────────────────────

  const statusListeners = new Set<(event: LinearStatusEvent) => void>();

  const broadcastStatus = () => {
    const state = Effect.runSyncWith(context)(SynchronizedRef.get(stateRef));
    const snapshot = buildSnapshot(state);
    for (const listener of statusListeners) {
      listener(snapshot);
    }
  };

  // ── Polling ───────────────────────────────────────────────────────────

  // A single forked fiber owns the poll loop. Restarting swaps the fiber so the
  // next poll is a full interval from now, matching the old setInterval reset.
  let pollFiber: Fiber.Fiber<void, never> | null = null;

  const fetchAndUpdate = (token: string) =>
    Effect.gen(function* () {
      const [issues, labels] = yield* Effect.all([
        fetchAssignedIssues(token),
        fetchLabels(token),
      ]).pipe(Effect.provideService(HttpClient.HttpClient, httpClient));
      yield* SynchronizedRef.set(stateRef, { issues, labels, connected: true });
      broadcastStatus();
    }).pipe(
      Effect.catch((cause) => Effect.logError("[linear] Poll failed", { cause })),
      Effect.ignoreCause({ log: true }),
    );

  const stopPolling = Effect.suspend(() => {
    const fiber = pollFiber;
    pollFiber = null;
    return fiber === null ? Effect.void : Fiber.interrupt(fiber).pipe(Effect.asVoid);
  });

  const startPolling = (token: string) =>
    Effect.gen(function* () {
      yield* stopPolling;
      // Fetch immediately, then poll on a fixed spacing.
      pollFiber = yield* fetchAndUpdate(token).pipe(
        Effect.repeat(Schedule.spaced(POLL_INTERVAL)),
        Effect.asVoid,
        Effect.forkIn(pollScope),
      );
    });

  // ── React to settings changes ──────────────────────────────────────────

  const settings = yield* serverSettings.getSettings;
  let currentToken = settings.linear.apiToken;

  if (currentToken) {
    yield* startPolling(currentToken);
  }

  // Watch for token changes in background
  runFork(
    Effect.gen(function* () {
      yield* serverSettings.streamChanges.pipe(
        Stream.runForEach((newSettings) =>
          Effect.gen(function* () {
            const newToken = newSettings.linear.apiToken;
            if (newToken === currentToken) return;

            currentToken = newToken;
            if (newToken) {
              yield* startPolling(newToken);
            } else {
              yield* stopPolling;
              yield* SynchronizedRef.set(stateRef, EMPTY_STATE);
              broadcastStatus();
            }
          }),
        ),
      );
    }),
  );

  // ── Cleanup ────────────────────────────────────────────────────────────

  yield* Effect.addFinalizer(() => stopPolling);

  // ── Service shape ──────────────────────────────────────────────────────

  return {
    list: () =>
      Effect.gen(function* () {
        if (!currentToken) {
          return { issues: [], labels: [], connected: false };
        }
        const state = yield* SynchronizedRef.get(stateRef);
        return buildSnapshot(state);
      }),

    refresh: () =>
      Effect.gen(function* () {
        if (!currentToken) {
          return { issues: [], labels: [], connected: false };
        }
        const [issues, labels] = yield* Effect.all([
          fetchAssignedIssues(currentToken),
          fetchLabels(currentToken),
        ]).pipe(Effect.provideService(HttpClient.HttpClient, httpClient));
        yield* SynchronizedRef.set(stateRef, { issues, labels, connected: true });
        broadcastStatus();
        // Restart polling timer so next poll is a full interval from now
        yield* startPolling(currentToken);
        return buildSnapshot({ issues, labels, connected: true });
      }),

    assignLabel: (input: LinearAssignLabelInput) =>
      Effect.gen(function* () {
        if (!currentToken) {
          return yield* new LinearNotConfiguredError({
            detail: "Linear API token not configured",
          });
        }

        const result: LinearAssignLabelResult = yield* assignIssueLabel(
          currentToken,
          input.issueId,
          input.labelId,
        ).pipe(Effect.provideService(HttpClient.HttpClient, httpClient));

        // Update local cache: update the issue's labels
        yield* SynchronizedRef.update(stateRef, (state) => ({
          ...state,
          issues: state.issues.map((issue) =>
            issue.id === input.issueId ? { ...issue, labels: result.labels } : issue,
          ),
        }));
        broadcastStatus();

        return result;
      }),

    streamStatus: Stream.callback<LinearStatusEvent>((queue) =>
      Effect.acquireRelease(
        Effect.sync(() => {
          // Send initial snapshot
          const state = Effect.runSyncWith(context)(SynchronizedRef.get(stateRef));
          Effect.runSyncWith(context)(Queue.offer(queue, buildSnapshot(state)));

          // Subscribe to future updates
          const listener = (event: LinearStatusEvent) => {
            Effect.runSyncWith(context)(Queue.offer(queue, event));
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
