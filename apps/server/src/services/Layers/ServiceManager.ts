/**
 * ServiceManagerLive - Layer implementation for development service orchestration.
 *
 * Manages Docker containers (via docker compose CLI) and local processes,
 * handles dependency-ordered startup, health polling, and task scheduling.
 *
 * @module ServiceManagerLive
 */
import {
  type ServiceActionInput,
  type ServiceLogEntry,
  type ServiceLogInput,
  type ServiceState,
  ServiceConfigError,
  ServiceDependencyError,
  ServiceLifecycleError,
  ServiceNotFoundError,
  type ServicesSnapshot,
  type ServicesStatusEvent,
  type ServiceStatus,
  type TaskActionInput,
  TaskLifecycleError,
  TaskNotFoundError,
  type TaskState,
  type TaskStatus,
} from "@t3tools/contracts";
import {
  Clock,
  Data,
  Duration,
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
import * as DateTime from "effect/DateTime";
import * as Schema from "effect/Schema";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as HttpClient from "effect/unstable/http/HttpClient";
import * as HttpClientRequest from "effect/unstable/http/HttpClientRequest";
import * as ChildProcess from "effect/unstable/process/ChildProcess";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";

import { ServerConfig } from "../../config.ts";
import * as ProcessRunner from "../../processRunner.ts";
import {
  isProcessAlive,
  killProcessGroup,
  readPidFile,
  removePidFile,
  writePidFile,
} from "../pidFile.ts";
import { ServiceManager, type ServiceManagerShape } from "../Services/ServiceManager.ts";
import {
  loadServiceConfig,
  parseDotenv,
  topologicalSort,
  type ServiceConfig,
  type ServiceDefConfig,
  type TaskDefConfig,
} from "./ServiceConfigLoader.ts";

const HEALTH_POLL_INTERVAL = "5 seconds";
const PROCESS_KILL_GRACE = "5 seconds";
const ADOPTED_PROCESS_POLL_INTERVAL = "200 millis";
const HEALTHY_WAIT_POLL_INTERVAL = "1 second";
const DOCKER_COMPOSE_TIMEOUT = "120 seconds";
const DOCKER_INSPECT_TIMEOUT = "5 seconds";
const HTTP_HEALTH_TIMEOUT = "3 seconds";
const TASK_COMMAND_TIMEOUT = "30 seconds";
const LOG_BUFFER_MAX_LINES = 500;

/** Shape of `docker inspect --format {{json .State}}` output. */
const DockerState = Schema.Struct({
  Running: Schema.optional(Schema.Boolean),
  Status: Schema.optional(Schema.String),
  Health: Schema.optional(Schema.Struct({ Status: Schema.optional(Schema.String) })),
});
const DockerStateFromJson = Schema.fromJsonString(DockerState);
const decodeDockerState = Schema.decodeUnknownEffect(DockerStateFromJson);

/** Internal lifecycle failure, mapped to a contract error at the RPC boundary. */
class ServiceOperationError extends Data.TaggedError("ServiceOperationError")<{
  readonly reason: string;
}> {
  override get message(): string {
    return this.reason;
  }
}

/** A process spawned through the Effect spawner. */
type SpawnedProcess = ChildProcessSpawner.ChildProcessHandle;

interface ServiceRuntime {
  status: ServiceStatus;
  startedAt?: number | undefined;
  error?: string | undefined;
  process?: SpawnedProcess | undefined;
  /** PID of an adopted orphan process (no spawned handle). */
  adoptedPid?: number | undefined;
  /** Fiber draining the spawned process's output into the log buffer. */
  logFiber?: Fiber.Fiber<void, never> | undefined;
  /** Docker log follower (docker compose logs -f) and its drain fiber. */
  logFollower?: SpawnedProcess | undefined;
  logFollowerFiber?: Fiber.Fiber<void, never> | undefined;
}

interface TaskRuntime {
  status: TaskStatus;
  intervalFiber?: Fiber.Fiber<void, never> | undefined;
  lastRunAt?: string | undefined;
  error?: string | undefined;
}

interface ManagerState {
  config: ServiceConfig | null;
  services: Map<string, ServiceRuntime>;
  tasks: Map<string, TaskRuntime>;
}

function makeServiceState(
  id: string,
  def: ServiceDefConfig,
  runtime: ServiceRuntime,
  now: number,
): ServiceState {
  return {
    id,
    type: def.type,
    status: runtime.status,
    ports: [...def.ports],
    depends: [...def.depends],
    uptimeMs: runtime.startedAt ? now - runtime.startedAt : undefined,
    error: runtime.error,
  };
}

function makeTaskState(id: string, def: TaskDefConfig, runtime: TaskRuntime): TaskState {
  return {
    id,
    status: runtime.status,
    intervalSeconds: def.intervalSeconds,
    depends: [...def.depends],
    lastRunAt: runtime.lastRunAt,
    error: runtime.error,
  };
}

function buildSnapshot(state: ManagerState, now: number): ServicesSnapshot {
  const config = state.config;
  if (!config) {
    return { services: [], tasks: [], configLoaded: false };
  }

  const services: ServiceState[] = [];
  for (const [id, def] of config.services) {
    const runtime = state.services.get(id) ?? { status: "stopped" as const };
    services.push(makeServiceState(id, def, runtime, now));
  }

  const tasks: TaskState[] = [];
  for (const [id, def] of config.tasks) {
    const runtime = state.tasks.get(id) ?? { status: "stopped" as const };
    tasks.push(makeTaskState(id, def, runtime));
  }

  return { services, tasks, configLoaded: true };
}

const checkProcessHealth = Effect.fn("ServiceManager.checkProcessHealth")(function* (
  runtime: ServiceRuntime,
  def: ServiceDefConfig,
) {
  const pid = runtime.process?.pid ?? runtime.adoptedPid;
  if (!pid) return "stopped";

  // Check if process is alive
  if (!isProcessAlive(pid)) return "stopped";

  // If HTTP health check is configured, try it
  if (def.healthCheck?.type === "http" && def.healthCheck.url) {
    const httpClient = yield* HttpClient.HttpClient;
    const status = yield* httpClient.execute(HttpClientRequest.get(def.healthCheck.url)).pipe(
      Effect.timeout(HTTP_HEALTH_TIMEOUT),
      Effect.map((response) => (response.status < 400 ? "healthy" : "unhealthy") as ServiceStatus),
      // Process alive but health endpoint not ready.
      Effect.orElseSucceed(() => "running" as ServiceStatus),
    );
    return status;
  }

  return "running";
});

/** Stop a service's docker log follower and its drain fiber. */
const stopDockerLogFollower = Effect.fn("ServiceManager.stopDockerLogFollower")(function* (
  runtime: ServiceRuntime,
) {
  if (runtime.logFollower) {
    yield* runtime.logFollower.kill().pipe(Effect.ignore);
    runtime.logFollower = undefined;
  }
  if (runtime.logFollowerFiber) {
    yield* Fiber.interrupt(runtime.logFollowerFiber).pipe(Effect.ignore);
    runtime.logFollowerFiber = undefined;
  }
});

const makeServiceManager = Effect.fn("makeServiceManager")(function* () {
  const serverConfig = yield* ServerConfig;
  const context = yield* Effect.context<never>();
  const runFork = Effect.runForkWith(context);
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const processRunner = yield* ProcessRunner.ProcessRunner;
  const httpClient = yield* HttpClient.HttpClient;
  const runSync = Effect.runSyncWith(context);

  // Long-lived fibers (health poll, log drains, task loops) live in a scope of
  // their own so they are torn down with the layer.
  const supervisionScope = yield* Scope.make("sequential");
  yield* Effect.addFinalizer(() => Scope.close(supervisionScope, Exit.void));

  // PID-file and config helpers need FileSystem/Path. Capture them once so the
  // service methods stay free of those requirements.
  const ioEnv = yield* Effect.context<FileSystem.FileSystem | Path.Path>();
  const provideIo = <A, E>(effect: Effect.Effect<A, E, FileSystem.FileSystem | Path.Path>) =>
    effect.pipe(Effect.provideContext(ioEnv));

  const stateRef = yield* SynchronizedRef.make<ManagerState>({
    config: null,
    services: new Map(),
    tasks: new Map(),
  });

  // ── Listeners ─────────────────────────────────────────────────────────

  const statusListeners = new Set<(event: ServicesStatusEvent) => void>();

  const broadcastStatus = () => {
    const state = runSync(SynchronizedRef.get(stateRef));
    const now = runSync(Clock.currentTimeMillis);
    const snapshot = buildSnapshot(state, now);
    for (const listener of statusListeners) {
      listener(snapshot);
    }
  };

  // ── Log capture ──────────────────────────────────────────────────────

  const logBuffers = new Map<string, ServiceLogEntry[]>();
  const logListeners = new Map<string, Set<(entry: ServiceLogEntry) => void>>();

  const appendLog = (serviceId: string, stream: "stdout" | "stderr", text: string) => {
    const entry: ServiceLogEntry = {
      serviceId,
      stream,
      text,
      timestamp: DateTime.formatIso(runSync(DateTime.now)),
    };

    let buffer = logBuffers.get(serviceId);
    if (!buffer) {
      buffer = [];
      logBuffers.set(serviceId, buffer);
    }
    buffer.push(entry);
    if (buffer.length > LOG_BUFFER_MAX_LINES) {
      buffer.splice(0, buffer.length - LOG_BUFFER_MAX_LINES);
    }

    const listeners = logListeners.get(serviceId);
    if (listeners) {
      for (const listener of listeners) {
        listener(entry);
      }
    }
  };

  const decoder = new TextDecoder();

  /** Drain a spawned process's stdout/stderr into the service's log buffer. */
  const drainLogs = (serviceId: string, child: SpawnedProcess) =>
    Effect.all(
      [
        Stream.runForEach(child.stdout, (chunk) =>
          Effect.sync(() => appendLog(serviceId, "stdout", decoder.decode(chunk))),
        ),
        Stream.runForEach(child.stderr, (chunk) =>
          Effect.sync(() => appendLog(serviceId, "stderr", decoder.decode(chunk))),
        ),
      ],
      { concurrency: 2 },
    ).pipe(Effect.asVoid, Effect.ignoreCause({ log: true }));

  const startDockerLogFollower = Effect.fn("ServiceManager.startDockerLogFollower")(function* (
    serviceId: string,
  ) {
    const composePath = config?.dockerComposePath;
    if (!composePath) return undefined;
    const projectName = config?.dockerProjectName ?? "lygos";

    const env = yield* freshEnv;
    const child = yield* spawner
      .spawn(
        ChildProcess.make(
          "docker",
          [
            "compose",
            "-f",
            composePath,
            "--project-name",
            projectName,
            "logs",
            "-f",
            "--tail=50",
            serviceId,
          ],
          { env, extendEnv: true },
        ),
      )
      .pipe(
        Scope.provide(supervisionScope),
        // A dead log follower is not critical.
        Effect.orElseSucceed(() => null),
      );
    if (child === null) return undefined;

    const fiber = yield* drainLogs(serviceId, child).pipe(Effect.forkIn(supervisionScope));
    return { child, fiber } as const;
  });

  // ── Config loading ────────────────────────────────────────────────────

  const config: ServiceConfig | null = yield* loadServiceConfig(serverConfig.cwd).pipe(
    Effect.orElseSucceed(() => null),
  );

  if (config) {
    yield* SynchronizedRef.update(stateRef, (s) => ({ ...s, config }));

    // Initialize runtime entries, detecting already-running services
    const serviceRuntimes = new Map<string, ServiceRuntime>();
    for (const [id, def] of config.services) {
      if (def.type === "process") {
        // Check for orphaned process via PID file
        const pid = yield* readPidFile(serverConfig.cwd, id);
        if (pid && isProcessAlive(pid)) {
          const startedAt = yield* Clock.currentTimeMillis;
          serviceRuntimes.set(id, { status: "running", startedAt, adoptedPid: pid });
          appendLog(
            id,
            "stdout",
            `Attached to existing process (PID ${pid}). Restart to enable log streaming.`,
          );
          continue;
        }
        // Stale PID file — clean up
        if (pid) yield* removePidFile(serverConfig.cwd, id);
      }
      serviceRuntimes.set(id, { status: "stopped" });
    }

    const taskRuntimes = new Map<string, TaskRuntime>();
    for (const id of config.tasks.keys()) {
      taskRuntimes.set(id, { status: "stopped" });
    }
    yield* SynchronizedRef.update(stateRef, (s) => ({
      ...s,
      services: serviceRuntimes,
      tasks: taskRuntimes,
    }));
  }

  // Re-read the envFile from disk on every spawn so `dev env pull <env>`
  // changes are picked up by a service start/restart without needing to
  // restart the t3code server itself.
  const freshEnv = Effect.suspend(() =>
    config?.envFile ? parseDotenv(config.envFile) : Effect.succeed({} as Record<string, string>),
  );

  // ── Docker helpers ────────────────────────────────────────────────────

  const dockerCompose = Effect.fn("ServiceManager.dockerCompose")(function* (args: string[]) {
    const composePath = config?.dockerComposePath;
    if (!composePath) {
      return yield* Effect.fail(
        new ServiceOperationError({ reason: "No dockerComposePath configured" }),
      );
    }
    const projectName = config?.dockerProjectName ?? "lygos";
    const env = yield* freshEnv;
    return yield* processRunner.run({
      command: "docker",
      args: ["compose", "-f", composePath, "--project-name", projectName, ...args],
      env,
      timeout: DOCKER_COMPOSE_TIMEOUT,
      timeoutBehavior: "timedOutResult",
    });
  });

  // ── Health checking ───────────────────────────────────────────────────

  const dockerContainerName = Effect.fn("ServiceManager.dockerContainerName")(function* (
    serviceId: string,
  ) {
    const result = yield* dockerCompose(["ps", "--format", "{{.Name}}", serviceId]).pipe(
      Effect.orElseSucceed(() => null),
    );
    if (result === null) {
      // Fallback to default naming convention
      const projectName = config?.dockerProjectName ?? "lygos";
      return `${projectName}-${serviceId}-1`;
    }
    const name = result.stdout.trim();
    return name || null;
  });

  const checkDockerHealth = Effect.fn("ServiceManager.checkDockerHealth")(function* (
    serviceId: string,
  ) {
    const container = yield* dockerContainerName(serviceId);
    if (!container) return "stopped";

    // Single inspect call with JSON output for reliable parsing
    const result = yield* processRunner
      .run({
        command: "docker",
        args: ["inspect", "--format", "{{json .State}}", container],
        timeout: DOCKER_INSPECT_TIMEOUT,
        timeoutBehavior: "timedOutResult",
      })
      // docker binary not found or spawn failure
      .pipe(Effect.orElseSucceed(() => null));
    if (result === null) return "stopped";

    // Non-zero exit = container doesn't exist
    if (result.timedOut || result.code !== 0) return "stopped";

    const state = yield* decodeDockerState(result.stdout.trim()).pipe(
      Effect.orElseSucceed(() => null),
    );
    if (state === null) return "stopped"; // Unparseable output

    if (!state.Running) return "stopped";

    // Container is running — check health if available
    const health = state.Health?.Status;
    if (health === "healthy") return "healthy";
    if (health === "unhealthy") return "unhealthy";
    if (health === "starting") return "starting";

    return "running";
  });

  // ── Health polling loop ───────────────────────────────────────────────

  const healthPollTick = Effect.gen(function* () {
    if (!config) return;

    const state = yield* SynchronizedRef.get(stateRef);
    let changed = false;

    for (const [id, def] of config.services) {
      const runtime = state.services.get(id);
      if (!runtime) continue;

      // Process services: skip when stopped/stopping — we own the lifecycle
      // via spawn + PID file, so an external "start" is not a thing we need
      // to detect mid-poll.
      //
      // Docker services: ALWAYS re-check, even when our internal state is
      // "stopped". The container can be brought up or down outside t3code
      // (e.g. `docker compose up` in another terminal), and the UI must
      // reflect that.
      if (def.type === "process") {
        if (runtime.status === "stopped" || runtime.status === "stopping") continue;
      } else if (runtime.status === "stopping") {
        continue;
      }

      const newStatus =
        def.type === "docker"
          ? yield* checkDockerHealth(id)
          : yield* checkProcessHealth(runtime, def).pipe(
              Effect.provideService(HttpClient.HttpClient, httpClient),
            );

      if (newStatus !== runtime.status) {
        const wasStopped = runtime.status === "stopped";
        runtime.status = newStatus;
        if (newStatus === "stopped") {
          yield* stopDockerLogFollower(runtime);
          runtime.process = undefined;
          runtime.adoptedPid = undefined;
          runtime.startedAt = undefined;
          yield* removePidFile(serverConfig.cwd, id);
        } else if (wasStopped && def.type === "docker") {
          // Container was started externally — attach a log follower and
          // record the detection time so the UI shows uptime.
          runtime.startedAt = yield* Clock.currentTimeMillis;
          if (!runtime.logFollower) {
            const follower = yield* startDockerLogFollower(id);
            runtime.logFollower = follower?.child;
            runtime.logFollowerFiber = follower?.fiber;
          }
        }
        changed = true;
      }
    }

    if (changed) {
      broadcastStatus();
    }
  }).pipe(Effect.ignoreCause({ log: true }));

  yield* healthPollTick.pipe(
    Effect.repeat(Schedule.spaced(HEALTH_POLL_INTERVAL)),
    Effect.asVoid,
    Effect.forkIn(supervisionScope),
  );

  // ── Service start/stop ────────────────────────────────────────────────

  const isServiceHealthy = Effect.fn("ServiceManager.isServiceHealthy")(function* (id: string) {
    const state = yield* SynchronizedRef.get(stateRef);
    const runtime = state.services.get(id);
    if (!runtime) return false;
    return runtime.status === "healthy" || runtime.status === "running";
  });

  const waitForHealthy = Effect.fn("ServiceManager.waitForHealthy")(function* (
    id: string,
    timeout: Duration.Input = "60 seconds",
  ) {
    yield* Effect.gen(function* () {
      while (true) {
        if (yield* isServiceHealthy(id)) return;
        yield* Effect.sleep(HEALTHY_WAIT_POLL_INTERVAL);
      }
    }).pipe(
      Effect.timeout(timeout),
      // Timing out is the documented outcome; callers continue regardless.
      Effect.catch(() => Effect.void),
    );
  });

  const adoptProcess = Effect.fn("ServiceManager.adoptProcess")(function* (
    serviceId: string,
    pid: number,
  ) {
    appendLog(
      serviceId,
      "stdout",
      `Attached to existing process (PID ${pid}). Restart to enable log streaming.`,
    );
    const startedAt = yield* Clock.currentTimeMillis;
    yield* SynchronizedRef.update(stateRef, (s) => {
      const svcs = new Map(s.services);
      svcs.set(serviceId, { status: "running", startedAt, adoptedPid: pid });
      return { ...s, services: svcs };
    });
    broadcastStatus();
  });

  const startServiceInternal = Effect.fn("ServiceManager.startServiceInternal")(function* (
    serviceId: string,
  ) {
    if (!config)
      return yield* Effect.fail(new ServiceOperationError({ reason: "No config loaded" }));
    const def = config.services.get(serviceId);
    if (!def)
      return yield* Effect.fail(
        new ServiceOperationError({ reason: `Service not found: ${serviceId}` }),
      );

    const state = yield* SynchronizedRef.get(stateRef);
    const runtime = state.services.get(serviceId);
    if (
      runtime &&
      (runtime.status === "running" ||
        runtime.status === "healthy" ||
        runtime.status === "starting")
    ) {
      return; // Already running
    }

    // Check for an orphaned process from a previous session
    if (def.type === "process") {
      const existingPid = yield* readPidFile(serverConfig.cwd, serviceId);
      if (existingPid && isProcessAlive(existingPid)) {
        yield* adoptProcess(serviceId, existingPid);
        return;
      }
      // Stale PID file — clean up
      if (existingPid) yield* removePidFile(serverConfig.cwd, serviceId);
    }

    // Update status to starting
    yield* SynchronizedRef.update(stateRef, (s) => {
      const svcs = new Map(s.services);
      svcs.set(serviceId, {
        ...svcs.get(serviceId)!,
        status: "starting",
        error: undefined,
      });
      return { ...s, services: svcs };
    });
    broadcastStatus();

    if (def.type === "docker") {
      const result = yield* dockerCompose(["up", "-d", "--build", serviceId]);
      if (result.stderr && result.stderr.includes("Error")) {
        yield* SynchronizedRef.update(stateRef, (s) => {
          const svcs = new Map(s.services);
          svcs.set(serviceId, { status: "error", error: result.stderr });
          return { ...s, services: svcs };
        });
        broadcastStatus();
        return yield* Effect.fail(new ServiceOperationError({ reason: result.stderr }));
      }

      const follower = yield* startDockerLogFollower(serviceId);
      const startedAt = yield* Clock.currentTimeMillis;
      yield* SynchronizedRef.update(stateRef, (s) => {
        const svcs = new Map(s.services);
        svcs.set(serviceId, {
          status: "running",
          startedAt,
          logFollower: follower?.child,
          logFollowerFiber: follower?.fiber,
        });
        return { ...s, services: svcs };
      });
    } else {
      // Local process — spawn in its own process group
      const cwd = def.cwd;
      if (!cwd) {
        return yield* Effect.fail(
          new ServiceOperationError({
            reason: `No cwd configured for process service: ${serviceId}`,
          }),
        );
      }

      const env = yield* freshEnv;
      const child = yield* spawner
        .spawn(
          ChildProcess.make(def.command ?? "echo 'no command'", [], {
            cwd,
            shell: true,
            // Detach so the child gets its own process group and T3 Code can
            // exit without waiting for it.
            detached: true,
            env: { PORT: undefined, ...env },
            extendEnv: true,
          }),
        )
        .pipe(Scope.provide(supervisionScope));

      // Write PID file for recovery
      yield* writePidFile(serverConfig.cwd, serviceId, child.pid);

      const logFiber = yield* drainLogs(serviceId, child).pipe(Effect.forkIn(supervisionScope));

      // Watch for exit and reflect it in state.
      yield* child.exitCode.pipe(
        Effect.flatMap((code) =>
          Effect.gen(function* () {
            yield* removePidFile(serverConfig.cwd, serviceId);
            yield* SynchronizedRef.update(stateRef, (s) => {
              const svcs = new Map(s.services);
              svcs.set(serviceId, {
                status: code === 0 ? "stopped" : "error",
                error: code !== 0 ? `Process exited with code ${code}` : undefined,
              });
              return { ...s, services: svcs };
            });
            broadcastStatus();
          }),
        ),
        Effect.catch((cause) =>
          Effect.gen(function* () {
            yield* removePidFile(serverConfig.cwd, serviceId);
            yield* SynchronizedRef.update(stateRef, (s) => {
              const svcs = new Map(s.services);
              svcs.set(serviceId, { status: "error", error: String(cause) });
              return { ...s, services: svcs };
            });
            broadcastStatus();
          }),
        ),
        Effect.forkIn(supervisionScope),
      );

      const startedAt = yield* Clock.currentTimeMillis;
      yield* SynchronizedRef.update(stateRef, (s) => {
        const svcs = new Map(s.services);
        svcs.set(serviceId, { status: "running", startedAt, process: child, logFiber });
        return { ...s, services: svcs };
      });
    }

    broadcastStatus();
  });

  const stopServiceInternal = Effect.fn("ServiceManager.stopServiceInternal")(function* (
    serviceId: string,
  ) {
    if (!config)
      return yield* Effect.fail(new ServiceOperationError({ reason: "No config loaded" }));
    const def = config.services.get(serviceId);
    if (!def)
      return yield* Effect.fail(
        new ServiceOperationError({ reason: `Service not found: ${serviceId}` }),
      );

    const state = yield* SynchronizedRef.get(stateRef);
    const runtime = state.services.get(serviceId);
    if (!runtime || runtime.status === "stopped") return;

    yield* SynchronizedRef.update(stateRef, (s) => {
      const svcs = new Map(s.services);
      svcs.set(serviceId, { ...svcs.get(serviceId)!, status: "stopping" });
      return { ...s, services: svcs };
    });
    broadcastStatus();

    // Kill log follower if running
    yield* stopDockerLogFollower(runtime);

    if (def.type === "docker") {
      yield* dockerCompose(["stop", serviceId]);
    } else {
      const pid = runtime.process?.pid ?? runtime.adoptedPid;
      if (pid) {
        // Kill the entire process group (SIGTERM, then SIGKILL after grace period)
        killProcessGroup(pid, "SIGTERM");

        // Wait for the process to die, escalating to SIGKILL after the grace
        // period. Spawned children expose exitCode; adopted ones are polled.
        const awaitExit = runtime.process
          ? runtime.process.exitCode.pipe(Effect.asVoid, Effect.ignore)
          : Effect.gen(function* () {
              while (isProcessAlive(pid)) {
                yield* Effect.sleep(ADOPTED_PROCESS_POLL_INTERVAL);
              }
            });

        yield* awaitExit.pipe(
          Effect.timeout(PROCESS_KILL_GRACE),
          Effect.catch(() => Effect.sync(() => killProcessGroup(pid, "SIGKILL"))),
        );

        if (runtime.logFiber) {
          yield* Fiber.interrupt(runtime.logFiber).pipe(Effect.ignore);
          runtime.logFiber = undefined;
        }
      }
      yield* removePidFile(serverConfig.cwd, serviceId);
    }

    yield* SynchronizedRef.update(stateRef, (s) => {
      const svcs = new Map(s.services);
      svcs.set(serviceId, { status: "stopped" });
      return { ...s, services: svcs };
    });
    broadcastStatus();
  });

  // ── Task start/stop ───────────────────────────────────────────────────

  const startTaskInternal = Effect.fn("ServiceManager.startTaskInternal")(function* (
    taskId: string,
  ) {
    if (!config)
      return yield* Effect.fail(new ServiceOperationError({ reason: "No config loaded" }));
    const def = config.tasks.get(taskId);
    if (!def)
      return yield* Effect.fail(new ServiceOperationError({ reason: `Task not found: ${taskId}` }));

    const state = yield* SynchronizedRef.get(stateRef);
    const taskRuntime = state.tasks.get(taskId);
    if (taskRuntime?.status === "running") return;

    const runTick = Effect.gen(function* () {
      // Check dependencies are healthy
      for (const dep of def.depends) {
        if (!(yield* isServiceHealthy(dep))) return;
      }

      const env = yield* freshEnv;
      const result = yield* processRunner
        .run({
          command: "/bin/sh",
          args: ["-c", def.command],
          env,
          timeout: TASK_COMMAND_TIMEOUT,
          timeoutBehavior: "timedOutResult",
        })
        .pipe(Effect.catch((cause) => Effect.succeed({ failure: String(cause) } as const)));

      const failure =
        "failure" in result
          ? result.failure
          : result.timedOut
            ? "Task timed out"
            : result.code !== 0
              ? result.stderr.trim() || `Task exited with code ${result.code}`
              : undefined;

      const now = DateTime.formatIso(yield* DateTime.now);
      yield* SynchronizedRef.update(stateRef, (s) => {
        const tasks = new Map(s.tasks);
        const current = tasks.get(taskId);
        if (current?.status === "running") {
          tasks.set(taskId, { ...current, lastRunAt: now, error: failure });
        }
        return { ...s, tasks };
      });
      broadcastStatus();
    }).pipe(Effect.ignoreCause({ log: true }));

    const intervalFiber = yield* runTick.pipe(
      Effect.delay(Duration.seconds(def.intervalSeconds)),
      Effect.forever,
      Effect.asVoid,
      Effect.forkIn(supervisionScope),
    );

    yield* SynchronizedRef.update(stateRef, (s) => {
      const tasks = new Map(s.tasks);
      tasks.set(taskId, { status: "running", intervalFiber });
      return { ...s, tasks };
    });
    broadcastStatus();
  });

  const stopTaskInternal = Effect.fn("ServiceManager.stopTaskInternal")(function* (taskId: string) {
    const state = yield* SynchronizedRef.get(stateRef);
    const taskRuntime = state.tasks.get(taskId);
    if (!taskRuntime || taskRuntime.status === "stopped") return;

    if (taskRuntime.intervalFiber) {
      yield* Fiber.interrupt(taskRuntime.intervalFiber).pipe(Effect.ignore);
    }

    yield* SynchronizedRef.update(stateRef, (s) => {
      const tasks = new Map(s.tasks);
      tasks.set(taskId, { status: "stopped" });
      return { ...s, tasks };
    });
    broadcastStatus();
  });

  // ── Cleanup on shutdown ───────────────────────────────────────────────

  yield* Effect.addFinalizer(() =>
    Effect.gen(function* () {
      // Closing the supervision scope interrupts the health poll, log drains
      // and task loops.
      const state = yield* SynchronizedRef.get(stateRef);

      // Fire-and-forget SIGTERM to all process groups and log followers.
      // PID files are intentionally left for next startup recovery.
      for (const [, runtime] of state.services) {
        yield* stopDockerLogFollower(runtime);
        const pid = runtime.process?.pid ?? runtime.adoptedPid;
        if (pid) {
          killProcessGroup(pid, "SIGTERM");
        }
      }
    }).pipe(Effect.ignoreCause({ log: true })),
  );

  // ── Startup detection & auto-start ──────────────────────────────────

  if (config) {
    const dockerServices = [...config.services.entries()].filter(
      ([, def]) => def.type === "docker",
    );
    const autoStartServices = [...config.services.entries()]
      .filter(([, def]) => def.autoStart)
      .map(([id]) => id);
    const autoStartTasks = [...config.tasks.entries()]
      .filter(([, def]) => def.autoStart)
      .map(([id]) => id);

    // Run detection + auto-start in background (don't block layer construction)
    runFork(
      provideIo(
        Effect.gen(function* () {
          // Detect running Docker containers
          for (const [id] of dockerServices) {
            const status = yield* checkDockerHealth(id).pipe(
              // Docker not available or container doesn't exist
              Effect.orElseSucceed(() => "stopped" as ServiceStatus),
            );
            if (status !== "stopped") {
              const follower = yield* startDockerLogFollower(id);
              const startedAt = yield* Clock.currentTimeMillis;
              yield* SynchronizedRef.update(stateRef, (s) => {
                const svcs = new Map(s.services);
                svcs.set(id, {
                  status,
                  startedAt,
                  logFollower: follower?.child,
                  logFollowerFiber: follower?.fiber,
                });
                return { ...s, services: svcs };
              });
            }
          }
          broadcastStatus();

          // Auto-start services
          for (const id of autoStartServices) {
            const deps = topologicalSort(id, config!.services);
            for (const depId of deps) {
              // Log and continue on failure.
              yield* startServiceInternal(depId).pipe(Effect.ignoreCause({ log: true }));
              yield* waitForHealthy(depId).pipe(Effect.ignoreCause({ log: true }));
            }
          }
          for (const id of autoStartTasks) {
            yield* startTaskInternal(id).pipe(Effect.ignoreCause({ log: true }));
          }
        }).pipe(Effect.ignoreCause({ log: true })),
      ),
    );
  }

  // ── Service shape implementation ──────────────────────────────────────

  return {
    list: () =>
      Effect.gen(function* () {
        const state = yield* SynchronizedRef.get(stateRef);
        const now = yield* Clock.currentTimeMillis;
        return buildSnapshot(state, now);
      }),

    start: (input: ServiceActionInput) =>
      provideIo(
        Effect.gen(function* () {
          if (!config)
            return yield* new ServiceConfigError({ reason: "No lygos-services.yaml found" });
          const def = config.services.get(input.serviceId);
          if (!def) return yield* new ServiceNotFoundError({ serviceId: input.serviceId });

          // Start dependencies first (topological order)
          const startOrder = topologicalSort(input.serviceId, config.services);
          for (const depId of startOrder) {
            yield* Effect.gen(function* () {
              yield* startServiceInternal(depId);
              if (depId !== input.serviceId) {
                yield* waitForHealthy(depId);
              }
            }).pipe(
              Effect.mapError(
                (cause) =>
                  new ServiceLifecycleError({
                    serviceId: depId,
                    operation: "start",
                    reason: String(cause),
                  }),
              ),
            );
          }

          const state = yield* SynchronizedRef.get(stateRef);
          const runtime = state.services.get(input.serviceId) ?? { status: "stopped" as const };
          const now = yield* Clock.currentTimeMillis;
          return makeServiceState(input.serviceId, def, runtime, now);
        }),
      ),

    stop: (input: ServiceActionInput) =>
      provideIo(
        Effect.gen(function* () {
          if (!config)
            return yield* new ServiceConfigError({ reason: "No lygos-services.yaml found" });
          const def = config.services.get(input.serviceId);
          if (!def) return yield* new ServiceNotFoundError({ serviceId: input.serviceId });

          // Check for active dependents
          const state = yield* SynchronizedRef.get(stateRef);
          const activeDependents: string[] = [];
          for (const [otherId, otherDef] of config.services) {
            if (otherId === input.serviceId) continue;
            if (otherDef.depends.includes(input.serviceId)) {
              const otherRuntime = state.services.get(otherId);
              if (otherRuntime && otherRuntime.status !== "stopped") {
                activeDependents.push(otherId);
              }
            }
          }
          if (activeDependents.length > 0) {
            return yield* new ServiceDependencyError({
              serviceId: input.serviceId,
              dependents: activeDependents,
            });
          }

          yield* stopServiceInternal(input.serviceId).pipe(
            Effect.mapError(
              (cause) =>
                new ServiceLifecycleError({
                  serviceId: input.serviceId,
                  operation: "stop",
                  reason: String(cause),
                }),
            ),
          );

          const newState = yield* SynchronizedRef.get(stateRef);
          const runtime = newState.services.get(input.serviceId) ?? { status: "stopped" as const };
          const now = yield* Clock.currentTimeMillis;
          return makeServiceState(input.serviceId, def, runtime, now);
        }),
      ),

    restart: (input: ServiceActionInput) =>
      provideIo(
        Effect.gen(function* () {
          if (!config)
            return yield* new ServiceConfigError({ reason: "No lygos-services.yaml found" });
          const def = config.services.get(input.serviceId);
          if (!def) return yield* new ServiceNotFoundError({ serviceId: input.serviceId });

          yield* Effect.gen(function* () {
            yield* stopServiceInternal(input.serviceId);
            yield* startServiceInternal(input.serviceId);
          }).pipe(
            Effect.mapError(
              (cause) =>
                new ServiceLifecycleError({
                  serviceId: input.serviceId,
                  operation: "restart",
                  reason: String(cause),
                }),
            ),
          );

          const state = yield* SynchronizedRef.get(stateRef);
          const runtime = state.services.get(input.serviceId) ?? { status: "stopped" as const };
          const now = yield* Clock.currentTimeMillis;
          return makeServiceState(input.serviceId, def, runtime, now);
        }),
      ),

    startTask: (input: TaskActionInput) =>
      provideIo(
        Effect.gen(function* () {
          if (!config)
            return yield* new ServiceConfigError({ reason: "No lygos-services.yaml found" });
          const def = config.tasks.get(input.taskId);
          if (!def) return yield* new TaskNotFoundError({ taskId: input.taskId });

          yield* startTaskInternal(input.taskId).pipe(
            Effect.mapError(
              (cause) =>
                new TaskLifecycleError({
                  taskId: input.taskId,
                  operation: "start",
                  reason: String(cause),
                }),
            ),
          );

          const state = yield* SynchronizedRef.get(stateRef);
          const runtime = state.tasks.get(input.taskId) ?? { status: "stopped" as const };
          return makeTaskState(input.taskId, def, runtime);
        }),
      ),

    stopTask: (input: TaskActionInput) =>
      provideIo(
        Effect.gen(function* () {
          if (!config)
            return yield* new ServiceConfigError({ reason: "No lygos-services.yaml found" });
          const def = config.tasks.get(input.taskId);
          if (!def) return yield* new TaskNotFoundError({ taskId: input.taskId });

          yield* stopTaskInternal(input.taskId).pipe(
            Effect.mapError(
              (cause) =>
                new TaskLifecycleError({
                  taskId: input.taskId,
                  operation: "stop",
                  reason: String(cause),
                }),
            ),
          );

          const state = yield* SynchronizedRef.get(stateRef);
          const runtime = state.tasks.get(input.taskId) ?? { status: "stopped" as const };
          return makeTaskState(input.taskId, def, runtime);
        }),
      ),

    getLogs: (input: ServiceLogInput) =>
      Effect.gen(function* () {
        if (!config)
          return yield* new ServiceConfigError({ reason: "No lygos-services.yaml found" });
        if (!config.services.has(input.serviceId))
          return yield* new ServiceNotFoundError({ serviceId: input.serviceId });
        return logBuffers.get(input.serviceId) ?? [];
      }),

    streamLogs: (input: ServiceLogInput) =>
      Stream.callback<ServiceLogEntry>((queue) =>
        Effect.acquireRelease(
          Effect.sync(() => {
            // Replay buffered logs
            const buffer = logBuffers.get(input.serviceId) ?? [];
            for (const entry of buffer) {
              Effect.runSyncWith(context)(Queue.offer(queue, entry));
            }

            // Subscribe to live logs
            const listener = (entry: ServiceLogEntry) => {
              Effect.runSyncWith(context)(Queue.offer(queue, entry));
            };
            let listeners = logListeners.get(input.serviceId);
            if (!listeners) {
              listeners = new Set();
              logListeners.set(input.serviceId, listeners);
            }
            listeners.add(listener);
            return listener;
          }),
          (listener) =>
            Effect.sync(() => {
              const listeners = logListeners.get(input.serviceId);
              if (listeners) {
                listeners.delete(listener);
              }
            }),
        ),
      ),

    streamStatus: Stream.callback<ServicesStatusEvent>((queue) =>
      Effect.acquireRelease(
        Effect.sync(() => {
          // Send initial snapshot
          const state = Effect.runSyncWith(context)(SynchronizedRef.get(stateRef));
          const now = runSync(Clock.currentTimeMillis);
          runSync(Queue.offer(queue, buildSnapshot(state, now)));

          // Subscribe to future updates
          const listener = (event: ServicesStatusEvent) => {
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
  } satisfies ServiceManagerShape;
});

export const ServiceManagerLive = Layer.effect(ServiceManager, makeServiceManager());
