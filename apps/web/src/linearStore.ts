/**
 * Zustand store for Linear ticket management state.
 *
 * Populated via the linear.onStatus WebSocket subscription and
 * consumed by the Linear panel UI and sidebar status indicator.
 */
import {
  type LinearIssue,
  type LinearProject,
  type LinearSnapshot,
  type ProjectId,
  type ThreadId,
} from "@t3tools/contracts";
import { create } from "zustand";
import { createJSONStorage, persist } from "zustand/middleware";

export interface LinkedThread {
  threadId: ThreadId;
  projectId: ProjectId;
}

export interface LinearStoreState {
  issues: Record<string, LinearIssue>;
  projects: Record<string, LinearProject>;
  connected: boolean;

  /** Maps Linear issue ID → Lygos thread that was started from it. */
  linkedThreads: Record<string, LinkedThread>;

  applySnapshot: (snapshot: LinearSnapshot) => void;
  linkThread: (issueId: string, threadId: ThreadId, projectId: ProjectId) => void;
  unlinkThread: (issueId: string) => void;
}

export const useLinearStore = create<LinearStoreState>()(
  persist(
    (set) => ({
      issues: {},
      projects: {},
      connected: false,
      linkedThreads: {},

      applySnapshot: (snapshot) => {
        const issues: Record<string, LinearIssue> = {};
        for (const issue of snapshot.issues) {
          issues[issue.id] = issue;
        }
        const projects: Record<string, LinearProject> = {};
        for (const project of snapshot.projects) {
          projects[project.id] = project;
        }
        set({ issues, projects, connected: snapshot.connected });
      },

      linkThread: (issueId, threadId, projectId) => {
        set((state) => ({
          linkedThreads: { ...state.linkedThreads, [issueId]: { threadId, projectId } },
        }));
      },

      unlinkThread: (issueId) => {
        set((state) => {
          const { [issueId]: _, ...rest } = state.linkedThreads;
          return { linkedThreads: rest };
        });
      },
    }),
    {
      name: "linear-linked-threads",
      storage: createJSONStorage(() => localStorage),
      partialize: (state) => ({ linkedThreads: state.linkedThreads }),
    },
  ),
);
