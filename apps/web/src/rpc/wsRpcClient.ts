/**
 * Fork-local compatibility shim for the Services, Linear, and Setup panels.
 *
 * Those panels were written against a global, promise/callback-shaped
 * `WsRpcClient`. Upstream has since replaced that object with per-environment
 * Effect primitives (`EnvironmentRegistry` plus `request`/`subscribe`, driven
 * through `connectionAtomRuntime`). Rather than rewrite three panels around
 * atoms, this module reimplements the slice of the old surface they use on top
 * of the new primitives, resolved against the primary environment.
 *
 * Everything here is fork-only. If the panels are reworked around environments,
 * this file goes away.
 */
import {
  EnvironmentNotRegisteredError,
  EnvironmentRegistry,
  EnvironmentSupervisor,
} from "@t3tools/client-runtime/connection";
import { request, subscribe } from "@t3tools/client-runtime/rpc";
import { executeAtomQuery } from "@t3tools/client-runtime/state/runtime";
import {
  type EnvironmentId,
  type LinearAssignLabelInput,
  type LinearAssignLabelResult,
  type LinearSnapshot,
  type ServiceActionInput,
  type ServiceLogEntry,
  type ServiceLogInput,
  type ServiceState,
  type ServicesSnapshot,
  type SetupCheckInput,
  type SetupSnapshot,
  type TaskActionInput,
  type TaskState,
  WS_METHODS,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import { AsyncResult } from "effect/unstable/reactivity";

import { connectionAtomRuntime } from "../connection/runtime";
import { readPrimaryEnvironmentDescriptor } from "../environments/primary";
import { appAtomRegistry } from "./atomRegistry";

export class PrimaryEnvironmentUnavailableError extends Error {
  constructor() {
    super("No primary environment is connected yet.");
    this.name = "PrimaryEnvironmentUnavailableError";
  }
}

function primaryEnvironmentId(): EnvironmentId {
  const descriptor = readPrimaryEnvironmentDescriptor();
  if (!descriptor) {
    throw new PrimaryEnvironmentUnavailableError();
  }
  return descriptor.environmentId;
}

/** Run a unary RPC against the primary environment as a promise. */
async function unary<A, E>(
  build: (environmentId: EnvironmentId) => Effect.Effect<A, E, EnvironmentRegistry>,
): Promise<A> {
  const environmentId = primaryEnvironmentId();
  const atom = connectionAtomRuntime.atom(build(environmentId));
  const result = await executeAtomQuery(appAtomRegistry, atom, { reportFailure: false });
  if (AsyncResult.isFailure(result)) {
    throw Cause.squash(result.cause);
  }
  return result.value;
}

/**
 * Subscribe to an RPC stream on the primary environment.
 *
 * Returns an unsubscribe function. Mounting keeps the atom (and therefore the
 * underlying subscription) alive for as long as the caller holds it.
 */
function subscription<A, E>(
  build: (environmentId: EnvironmentId) => Stream.Stream<A, E, EnvironmentRegistry>,
  listener: (value: A) => void,
): () => void {
  let environmentId: EnvironmentId;
  try {
    environmentId = primaryEnvironmentId();
  } catch {
    // No primary environment yet; the panels resubscribe once one connects.
    return () => {};
  }

  const atom = connectionAtomRuntime.atom(build(environmentId));
  const unmount = appAtomRegistry.mount(atom);
  const unsubscribe = appAtomRegistry.subscribe(atom, (result) => {
    if (AsyncResult.isSuccess(result)) {
      listener(result.value);
    }
  });

  return () => {
    unsubscribe();
    unmount();
  };
}

const runOn = <A, E>(
  environmentId: EnvironmentId,
  effect: Effect.Effect<A, E, EnvironmentSupervisor>,
): Effect.Effect<A, E | EnvironmentNotRegisteredError, EnvironmentRegistry> =>
  EnvironmentRegistry.pipe(Effect.flatMap((registry) => registry.run(environmentId, effect)));

const streamOn = <A, E>(
  environmentId: EnvironmentId,
  stream: Stream.Stream<A, E, EnvironmentSupervisor>,
): Stream.Stream<A, E | EnvironmentNotRegisteredError, EnvironmentRegistry> =>
  Stream.unwrap(
    EnvironmentRegistry.pipe(Effect.map((registry) => registry.runStream(environmentId, stream))),
  );

export interface WsRpcClient {
  readonly setup: {
    readonly list: () => Promise<SetupSnapshot>;
    readonly check: (input: SetupCheckInput) => Promise<SetupSnapshot>;
    readonly onStatus: (listener: (snapshot: SetupSnapshot) => void) => () => void;
  };
  readonly linear: {
    readonly list: () => Promise<LinearSnapshot>;
    readonly refresh: () => Promise<LinearSnapshot>;
    readonly assignLabel: (input: LinearAssignLabelInput) => Promise<LinearAssignLabelResult>;
    readonly onStatus: (listener: (snapshot: LinearSnapshot) => void) => () => void;
  };
  readonly services: {
    readonly list: () => Promise<ServicesSnapshot>;
    readonly start: (input: ServiceActionInput) => Promise<ServiceState>;
    readonly stop: (input: ServiceActionInput) => Promise<ServiceState>;
    readonly restart: (input: ServiceActionInput) => Promise<ServiceState>;
    readonly startTask: (input: TaskActionInput) => Promise<TaskState>;
    readonly stopTask: (input: TaskActionInput) => Promise<TaskState>;
    readonly getLogs: (input: ServiceLogInput) => Promise<ReadonlyArray<ServiceLogEntry>>;
    readonly onStatus: (listener: (snapshot: ServicesSnapshot) => void) => () => void;
    readonly onLogs: (serviceId: string, listener: (entry: ServiceLogEntry) => void) => () => void;
  };
}

const client: WsRpcClient = {
  setup: {
    list: () => unary((id) => runOn(id, request(WS_METHODS.setupList, {}))),
    check: (input) => unary((id) => runOn(id, request(WS_METHODS.setupCheck, input))),
    onStatus: (listener) =>
      subscription((id) => streamOn(id, subscribe(WS_METHODS.subscribeSetupStatus, {})), listener),
  },
  linear: {
    list: () => unary((id) => runOn(id, request(WS_METHODS.linearList, {}))),
    refresh: () => unary((id) => runOn(id, request(WS_METHODS.linearRefresh, {}))),
    assignLabel: (input) => unary((id) => runOn(id, request(WS_METHODS.linearAssignLabel, input))),
    onStatus: (listener) =>
      subscription((id) => streamOn(id, subscribe(WS_METHODS.subscribeLinearStatus, {})), listener),
  },
  services: {
    list: () => unary((id) => runOn(id, request(WS_METHODS.servicesList, {}))),
    start: (input) => unary((id) => runOn(id, request(WS_METHODS.servicesStart, input))),
    stop: (input) => unary((id) => runOn(id, request(WS_METHODS.servicesStop, input))),
    restart: (input) => unary((id) => runOn(id, request(WS_METHODS.servicesRestart, input))),
    startTask: (input) => unary((id) => runOn(id, request(WS_METHODS.servicesStartTask, input))),
    stopTask: (input) => unary((id) => runOn(id, request(WS_METHODS.servicesStopTask, input))),
    getLogs: (input) => unary((id) => runOn(id, request(WS_METHODS.servicesGetLogs, input))),
    onStatus: (listener) =>
      subscription(
        (id) => streamOn(id, subscribe(WS_METHODS.subscribeServicesStatus, {})),
        listener,
      ),
    onLogs: (serviceId, listener) =>
      subscription(
        (id) => streamOn(id, subscribe(WS_METHODS.subscribeServiceLogs, { serviceId })),
        listener,
      ),
  },
};

export function getWsRpcClient(): WsRpcClient {
  return client;
}
