/**
 * PID file management for service process tracking.
 *
 * Writes PID files to `<cwd>/.lygos/services/<serviceId>.pid` so that
 * orphaned processes can be recovered across app restarts.
 *
 * @module pidFile
 */
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";

const PID_DIR_NAME = ".lygos/services";

const pidPath = (cwd: string, serviceId: string) =>
  Effect.gen(function* () {
    const path = yield* Path.Path;
    return path.join(cwd, PID_DIR_NAME, `${serviceId}.pid`);
  });

/** Write a PID file for a service. Creates the directory if needed. */
export const writePidFile = Effect.fn("pidFile.writePidFile")(function* (
  cwd: string,
  serviceId: string,
  pid: number,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const dir = path.join(cwd, PID_DIR_NAME);
  yield* fs.makeDirectory(dir, { recursive: true });
  yield* fs.writeFileString(yield* pidPath(cwd, serviceId), String(pid));
});

/** Read the PID from a service's PID file. Returns null if missing or invalid. */
export const readPidFile = Effect.fn("pidFile.readPidFile")(function* (
  cwd: string,
  serviceId: string,
) {
  const fs = yield* FileSystem.FileSystem;
  const content = yield* fs
    .readFileString(yield* pidPath(cwd, serviceId))
    .pipe(Effect.orElseSucceed(() => null));
  if (content === null) {
    return null;
  }
  const pid = Number.parseInt(content.trim(), 10);
  return Number.isNaN(pid) ? null : pid;
});

/** Remove a service's PID file. Missing files are not an error. */
export const removePidFile = Effect.fn("pidFile.removePidFile")(function* (
  cwd: string,
  serviceId: string,
) {
  const fs = yield* FileSystem.FileSystem;
  yield* fs
    .remove(yield* pidPath(cwd, serviceId), { force: true })
    .pipe(Effect.catch(() => Effect.void));
});

/** Check if a process with the given PID is alive. */
export function isProcessAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

/** Kill a process group (negative PID). Returns true if the signal was sent. */
export function killProcessGroup(pid: number, signal: NodeJS.Signals): boolean {
  try {
    process.kill(-pid, signal);
    return true;
  } catch {
    return false;
  }
}
