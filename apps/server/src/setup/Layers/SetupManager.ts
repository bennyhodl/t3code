/**
 * SetupManagerLive - Layer implementation for environment setup checks.
 *
 * Runs CLI commands to verify tools, authentication, repositories,
 * and environment configuration for Lygos development.
 *
 * @module SetupManagerLive
 */
import {
  type SetupCategory,
  type SetupCheckInput,
  type SetupCheckResult,
  type SetupCheckStatus,
  type SetupSnapshot,
  type SetupStatusEvent,
} from "@t3tools/contracts";
import { Cause, Effect, Layer, Queue, Stream, SynchronizedRef } from "effect";
import * as DateTime from "effect/DateTime";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";

import * as ProcessRunner from "../../processRunner.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import { SetupManager, type SetupManagerShape } from "../Services/SetupManager.ts";

/** Services every check needs. */
type CheckEnv = FileSystem.FileSystem | Path.Path | ProcessRunner.ProcessRunner;

// ── Command helpers ──────────────────────────────────────────────────

const DEFAULT_COMMAND_TIMEOUT = "5 seconds";

/**
 * Run a shell command, reporting success/failure rather than failing the
 * effect — every setup check treats a non-zero exit as "not configured".
 */
const tryExec = Effect.fn("SetupManager.tryExec")(function* (cmd: string) {
  const runner = yield* ProcessRunner.ProcessRunner;
  const result = yield* runner
    .run({
      command: "/bin/sh",
      args: ["-c", cmd],
      timeout: DEFAULT_COMMAND_TIMEOUT,
      timeoutBehavior: "timedOutResult",
    })
    .pipe(Effect.orElseSucceed(() => null));

  if (result === null || result.timedOut || result.code !== 0) {
    return { ok: false, stdout: "" };
  }
  return { ok: true, stdout: result.stdout.trim() };
});

const commandExists = Effect.fn("SetupManager.commandExists")(function* (binary: string) {
  const which = yield* tryExec(`which ${binary}`);
  if (!which.ok) return { found: false, version: undefined };
  const ver = yield* tryExec(`${binary} --version`);
  return { found: true, version: ver.ok ? ver.stdout.split("\n")[0] : undefined };
});

const fileExists = Effect.fn("SetupManager.fileExists")(function* (filePath: string) {
  const fs = yield* FileSystem.FileSystem;
  return yield* fs.exists(filePath).pipe(Effect.orElseSucceed(() => false));
});

const dirExists = Effect.fn("SetupManager.dirExists")(function* (dirPath: string) {
  const fs = yield* FileSystem.FileSystem;
  const info = yield* fs.stat(dirPath).pipe(Effect.orElseSucceed(() => null));
  return info !== null && info.type === "Directory";
});

// ── Check definitions ────────────────────────────────────────────────

interface CheckDef {
  id: string;
  name: string;
  description: string;
  required: boolean;
  category: SetupCategory;
  run: (ctx: CheckContext) => Effect.Effect<CheckOutcome, never, CheckEnv>;
}

interface CheckContext {
  lygosPath: string | undefined;
  linearToken: string | undefined;
}

interface CheckOutcome {
  status: SetupCheckStatus;
  detail?: string | undefined;
  fixCommand?: string | undefined;
}

const lygosPath = (): string | undefined => process.env.LYGOS_PATH || undefined;

// ── Environment checks ──────────────────────────────────────────────

const environmentChecks: CheckDef[] = [
  {
    id: "env-lygos-path",
    name: "LYGOS_PATH",
    description: "Root directory containing all Lygos repositories",
    required: true,
    category: "environment",
    run: (ctx) =>
      Effect.gen(function* () {
        if (!ctx.lygosPath) {
          return {
            status: "fail",
            detail: "Environment variable not set",
            fixCommand: "export LYGOS_PATH=~/src/lygoslabs",
          };
        }
        if (!(yield* dirExists(ctx.lygosPath))) {
          return {
            status: "fail",
            detail: `Directory does not exist: ${ctx.lygosPath}`,
            fixCommand: `mkdir -p ${ctx.lygosPath}`,
          };
        }
        return { status: "pass", detail: ctx.lygosPath };
      }),
  },
  {
    id: "env-lygos-dev",
    name: "lygos-dev .env",
    description: "Environment file for local dev services",
    required: true,
    category: "environment",
    run: (ctx) =>
      Effect.gen(function* () {
        const path = yield* Path.Path;
        if (!ctx.lygosPath) return { status: "fail", detail: "LYGOS_PATH not set" };
        const p = path.join(ctx.lygosPath, "lygos-dev", ".env");
        if (!(yield* fileExists(p))) {
          return { status: "fail", detail: "File not found", fixCommand: "dev env pull dev" };
        }
        return { status: "pass", detail: p };
      }),
  },
  {
    id: "env-orange-grove",
    name: "orange-grove/core .env",
    description: "Environment file for Orange Grove backend",
    required: true,
    category: "environment",
    run: (ctx) =>
      Effect.gen(function* () {
        const path = yield* Path.Path;
        if (!ctx.lygosPath) return { status: "fail", detail: "LYGOS_PATH not set" };
        const p = path.join(ctx.lygosPath, "orange-grove", "core", ".env");
        if (!(yield* fileExists(p))) {
          return { status: "fail", detail: "File not found", fixCommand: "dev env pull dev" };
        }
        return { status: "pass", detail: p };
      }),
  },
  {
    id: "env-dlcd-rs",
    name: "dlcd-rs .env",
    description: "Environment file for DLCD Rust service",
    required: true,
    category: "environment",
    run: (ctx) =>
      Effect.gen(function* () {
        const path = yield* Path.Path;
        if (!ctx.lygosPath) return { status: "fail", detail: "LYGOS_PATH not set" };
        const p = path.join(ctx.lygosPath, "dlcd-rs", ".env");
        if (!(yield* fileExists(p))) {
          return { status: "fail", detail: "File not found", fixCommand: "dev env pull dev" };
        }
        return { status: "pass", detail: p };
      }),
  },
  {
    id: "env-mocknolia",
    name: "mocknolia .env",
    description: "Environment file for mock oracle service",
    required: true,
    category: "environment",
    run: (ctx) =>
      Effect.gen(function* () {
        const path = yield* Path.Path;
        if (!ctx.lygosPath) return { status: "fail", detail: "LYGOS_PATH not set" };
        const p = path.join(ctx.lygosPath, "mocknolia", ".env");
        if (!(yield* fileExists(p))) {
          return { status: "fail", detail: "File not found", fixCommand: "dev env pull dev" };
        }
        return { status: "pass", detail: p };
      }),
  },
];

// ── Tool checks ─────────────────────────────────────────────────────

function toolCheck(
  id: string,
  name: string,
  binary: string,
  description: string,
  required: boolean,
  installCmd: string,
  versionCheck?: (version: string) => CheckOutcome | null,
): CheckDef {
  return {
    id,
    name,
    description,
    required,
    category: "tools",
    run: (_ctx) =>
      Effect.gen(function* () {
        const result = yield* commandExists(binary);
        if (!result.found) {
          return { status: "fail", detail: "Not found in PATH", fixCommand: installCmd };
        }
        if (versionCheck && result.version) {
          const override = versionCheck(result.version);
          if (override) return override;
        }
        return { status: "pass", detail: result.version ?? "Installed" };
      }),
  };
}

const toolChecks: CheckDef[] = [
  toolCheck(
    "tool-brew",
    "Homebrew",
    "brew",
    "Package manager",
    true,
    '/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"',
  ),
  toolCheck(
    "tool-node",
    "Node.js",
    "node",
    "JavaScript runtime (v20 required)",
    true,
    "nvm install 20",
    (ver) => {
      const match = ver.match(/v(\d+)\./);
      if (match?.[1]) {
        const major = parseInt(match[1], 10);
        if (major < 20) {
          return { status: "warn", detail: `${ver} — v20+ required`, fixCommand: "nvm install 20" };
        }
      }
      return null;
    },
  ),
  {
    id: "tool-nvm",
    name: "nvm",
    description: "Node version manager",
    required: true,
    category: "tools",
    run: (_ctx) =>
      Effect.gen(function* () {
        const path = yield* Path.Path;
        const nvmDir = process.env.NVM_DIR;
        if (nvmDir && (yield* fileExists(path.join(nvmDir, "nvm.sh")))) {
          return { status: "pass", detail: `NVM_DIR: ${nvmDir}` };
        }
        const home = process.env.HOME ?? "";
        if (yield* fileExists(path.join(home, ".nvm", "nvm.sh"))) {
          return { status: "pass", detail: `~/.nvm/nvm.sh` };
        }
        return {
          status: "fail",
          detail: "nvm not found",
          fixCommand:
            "curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.1/install.sh | bash",
        };
      }),
  },
  toolCheck(
    "tool-pnpm",
    "pnpm",
    "pnpm",
    "Package manager (orange-grove, mock-server)",
    true,
    "npm i -g pnpm",
  ),
  toolCheck(
    "tool-yarn",
    "yarn",
    "yarn",
    "Package manager (mocknolia, lygos-app)",
    true,
    "npm i -g yarn",
  ),
  toolCheck(
    "tool-docker",
    "Docker",
    "docker",
    "Container runtime",
    true,
    "brew install --cask docker",
  ),
  {
    id: "tool-docker-compose",
    name: "Docker Compose",
    description: "Container orchestration",
    required: true,
    category: "tools",
    run: (_ctx) =>
      Effect.gen(function* () {
        const result = yield* tryExec("docker compose version");
        if (!result.ok) {
          return {
            status: "fail",
            detail: "Not available",
            fixCommand: "brew install --cask docker",
          };
        }
        return { status: "pass", detail: result.stdout.split("\n")[0] };
      }),
  },
  toolCheck("tool-git", "Git", "git", "Version control", true, "brew install git"),
  toolCheck(
    "tool-gh",
    "GitHub CLI",
    "gh",
    "GitHub operations and PR management",
    true,
    "brew install gh",
  ),
  toolCheck(
    "tool-rustc",
    "Rust",
    "rustc",
    "Compiler for dlcd-rs",
    true,
    "curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh",
  ),
  toolCheck(
    "tool-cargo",
    "Cargo",
    "cargo",
    "Rust package manager",
    true,
    "curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh",
  ),
  toolCheck(
    "tool-op",
    "1Password CLI",
    "op",
    "Environment file management",
    true,
    "brew install 1password-cli",
  ),
  toolCheck("tool-npm", "npm", "npm", "Private @lygos package access", true, "nvm install 20"),
  toolCheck("tool-mprocs", "mprocs", "mprocs", "TUI process runner", false, "brew install mprocs"),
  toolCheck(
    "tool-claude",
    "Claude Code",
    "claude",
    "AI coding agent",
    false,
    "npm i -g @anthropic-ai/claude-code",
  ),
  toolCheck("tool-codex", "Codex", "codex", "AI coding agent", false, "npm i -g @openai/codex"),
  toolCheck(
    "tool-kubectl",
    "kubectl",
    "kubectl",
    "Kubernetes cluster management",
    false,
    "brew install kubectl",
  ),
  toolCheck("tool-cmake", "cmake", "cmake", "Build dependency", false, "brew install cmake"),
  toolCheck(
    "tool-protoc",
    "protobuf",
    "protoc",
    "Protocol buffer compiler",
    false,
    "brew install protobuf",
  ),
  toolCheck(
    "tool-fzf",
    "fzf",
    "fzf",
    "Fuzzy finder (used by dev dexec)",
    false,
    "brew install fzf",
  ),
  toolCheck("tool-uv", "uv", "uv", "Python package and version manager", false, "brew install uv"),
];

// ── Auth checks ─────────────────────────────────────────────────────

const authChecks: CheckDef[] = [
  {
    id: "auth-github",
    name: "GitHub",
    description: "LygosLabs organization access",
    required: true,
    category: "authentication",
    run: (_ctx) =>
      Effect.gen(function* () {
        const gh = yield* commandExists("gh");
        if (!gh.found)
          return {
            status: "fail",
            detail: "GitHub CLI not installed",
            fixCommand: "brew install gh",
          };
        const auth = yield* tryExec("gh auth status 2>&1");
        if (!auth.ok) {
          return { status: "warn", detail: "Not authenticated", fixCommand: "gh auth login" };
        }
        return { status: "pass", detail: "Authenticated" };
      }),
  },
  {
    id: "auth-gcloud",
    name: "Google Cloud",
    description: "GCP secret manager access",
    required: false,
    category: "authentication",
    run: (_ctx) =>
      Effect.gen(function* () {
        const gc = yield* commandExists("gcloud");
        if (!gc.found)
          return {
            status: "fail",
            detail: "gcloud CLI not installed",
            fixCommand: "brew install google-cloud-sdk",
          };
        const auth = yield* tryExec("gcloud auth application-default print-access-token 2>&1");
        if (!auth.ok) {
          return {
            status: "warn",
            detail: "Not authenticated",
            fixCommand: "gcloud auth application-default login",
          };
        }
        return { status: "pass", detail: "Authenticated" };
      }),
  },
  {
    id: "auth-linear",
    name: "Linear",
    description: "Linear issue tracking integration",
    required: false,
    category: "authentication",
    // Purely a settings lookup — no services needed.
    run: (ctx) =>
      Effect.succeed(
        ctx.linearToken
          ? { status: "pass", detail: "Token configured" }
          : {
              status: "warn",
              detail: "API token not configured",
              fixCommand: "Set token in Settings or Setup tab",
            },
      ),
  },
  {
    id: "auth-npm",
    name: "npm",
    description: "Private @lygos package access",
    required: true,
    category: "authentication",
    run: (_ctx) =>
      Effect.gen(function* () {
        const npm = yield* commandExists("npm");
        if (!npm.found)
          return { status: "fail", detail: "npm not installed", fixCommand: "nvm install 20" };
        const whoami = yield* tryExec("npm whoami 2>&1");
        if (!whoami.ok) {
          return { status: "warn", detail: "Not authenticated", fixCommand: "npm login" };
        }
        return { status: "pass", detail: `Logged in as ${whoami.stdout}` };
      }),
  },
];

// ── Repository checks ───────────────────────────────────────────────

const REPOS = [
  "lygos-dev",
  "lygos-app",
  "orange-grove",
  "electrs-batch-server",
  "mocknolia",
  "dlcd-rs",
  "mock-server",
] as const;

const repoChecks: CheckDef[] = REPOS.map((repo) => ({
  id: `repo-${repo}`,
  name: repo,
  description: `LygosLabs/${repo} repository`,
  required: true,
  category: "repositories" as const,
  run: (ctx: CheckContext) =>
    Effect.gen(function* () {
      const path = yield* Path.Path;
      if (!ctx.lygosPath) {
        return { status: "fail" as const, detail: "LYGOS_PATH not set" };
      }
      const repoPath = path.join(ctx.lygosPath, repo);
      if (!(yield* dirExists(repoPath))) {
        return {
          status: "fail" as const,
          detail: "Directory not found",
          fixCommand: `cd $LYGOS_PATH && git clone git@github.com:LygosLabs/${repo}.git`,
        };
      }
      if (!(yield* dirExists(path.join(repoPath, ".git")))) {
        return { status: "warn" as const, detail: "Directory exists but is not a git repository" };
      }
      return { status: "pass" as const, detail: repoPath };
    }),
}));

// ── All checks ──────────────────────────────────────────────────────

const ALL_CHECKS: CheckDef[] = [...environmentChecks, ...toolChecks, ...authChecks, ...repoChecks];

// ── Manager implementation ──────────────────────────────────────────

interface SetupState {
  checks: SetupCheckResult[];
  lastCheckedAt?: string;
  checking: boolean;
}

const EMPTY_STATE: SetupState = { checks: [], checking: false };

function buildSnapshot(state: SetupState): SetupSnapshot {
  return {
    checks: state.checks,
    lastCheckedAt: state.lastCheckedAt,
    checking: state.checking,
  };
}

const makeSetupManager = Effect.fn("makeSetupManager")(function* () {
  const serverSettings = yield* ServerSettingsService;
  const context = yield* Effect.context<never>();
  const runFork = Effect.runForkWith(context);

  // Checks need FileSystem/Path/ProcessRunner. Capture them once here so the
  // service methods stay free of those requirements.
  const checkEnv = yield* Effect.context<CheckEnv>();

  const stateRef = yield* SynchronizedRef.make<SetupState>(EMPTY_STATE);

  // ── Listeners ─────────────────────────────────────────────────────

  const statusListeners = new Set<(event: SetupStatusEvent) => void>();

  const broadcastStatus = () => {
    const state = Effect.runSyncWith(context)(SynchronizedRef.get(stateRef));
    const snapshot = buildSnapshot(state);
    for (const listener of statusListeners) {
      listener(snapshot);
    }
  };

  // ── Check runner ──────────────────────────────────────────────────

  let checkInProgress = false;

  const runChecks = Effect.fn("SetupManager.runChecks")(function* (categoryFilter?: SetupCategory) {
    if (checkInProgress) {
      yield* Effect.logDebug("[setup] Check already in progress, skipping");
      return;
    }
    checkInProgress = true;

    yield* Effect.logInfo(
      `[setup] Starting checks${categoryFilter ? ` (category: ${categoryFilter})` : " (all)"}`,
    );

    // Mark as checking and broadcast
    yield* SynchronizedRef.update(stateRef, (s) => ({ ...s, checking: true }));
    broadcastStatus();

    // Yield so the WebSocket can flush the "checking: true" message
    yield* Effect.yieldNow;

    const outcome = yield* Effect.gen(function* () {
      const settings = yield* serverSettings.getSettings;
      const ctx: CheckContext = {
        lygosPath: lygosPath(),
        linearToken: settings.linear.apiToken || undefined,
      };

      const checksToRun = categoryFilter
        ? ALL_CHECKS.filter((c) => c.category === categoryFilter)
        : ALL_CHECKS;

      // Run checks sequentially with event loop yields between them
      const results: SetupCheckResult[] = [];
      for (const def of checksToRun) {
        yield* Effect.logDebug(`[setup]   checking: ${def.id}`);
        const checkResult = yield* def.run(ctx).pipe(
          Effect.map((value) => ({ ok: true as const, value })),
          Effect.catchCause((cause) => Effect.succeed({ ok: false as const, cause })),
        );
        results.push(
          checkResult.ok
            ? {
                id: def.id,
                name: def.name,
                description: def.description,
                status: checkResult.value.status,
                required: def.required,
                category: def.category,
                detail: checkResult.value.detail,
                fixCommand: checkResult.value.fixCommand,
              }
            : {
                id: def.id,
                name: def.name,
                description: def.description,
                status: "fail" as const,
                required: def.required,
                category: def.category,
                detail: `Check failed: ${Cause.pretty(checkResult.cause)}`,
              },
        );
        // Yield between checks so WebSocket stays responsive
        yield* Effect.yieldNow;
      }

      const now = yield* DateTime.now;
      yield* SynchronizedRef.update(stateRef, (prev) => {
        let allChecks: SetupCheckResult[];
        if (categoryFilter) {
          const otherChecks = prev.checks.filter((c) => c.category !== categoryFilter);
          allChecks = [...otherChecks, ...results];
        } else {
          allChecks = results;
        }

        return {
          checks: allChecks,
          lastCheckedAt: DateTime.formatIso(now),
          checking: false,
        };
      });

      const passed = results.filter((r) => r.status === "pass").length;
      yield* Effect.logInfo(`[setup] Checks complete: ${passed}/${results.length} passed`);
    }).pipe(
      Effect.catchCause((cause) =>
        Effect.logError("[setup] Check failed", { cause }).pipe(
          Effect.andThen(SynchronizedRef.update(stateRef, (s) => ({ ...s, checking: false }))),
        ),
      ),
    );

    checkInProgress = false;
    broadcastStatus();
    return outcome;
  });

  // Run initial checks in background
  runFork(runChecks().pipe(Effect.provideContext(checkEnv)));

  // ── Service shape ─────────────────────────────────────────────────

  return {
    list: () =>
      Effect.gen(function* () {
        const state = yield* SynchronizedRef.get(stateRef);
        return buildSnapshot(state);
      }),

    check: (input: SetupCheckInput) =>
      Effect.gen(function* () {
        yield* runChecks(input.category).pipe(Effect.provideContext(checkEnv));
        const state = yield* SynchronizedRef.get(stateRef);
        return buildSnapshot(state);
      }),

    streamStatus: Stream.callback<SetupStatusEvent>((queue) =>
      Effect.acquireRelease(
        Effect.sync(() => {
          // Send initial snapshot
          const state = Effect.runSyncWith(context)(SynchronizedRef.get(stateRef));
          Effect.runSyncWith(context)(Queue.offer(queue, buildSnapshot(state)));

          // Subscribe to future updates
          const listener = (event: SetupStatusEvent) => {
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
  } satisfies SetupManagerShape;
});

export const SetupManagerLive = Layer.effect(SetupManager, makeSetupManager());
