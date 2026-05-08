/**
 * LinearGraphQLClient - Thin wrapper for making authenticated GraphQL
 * requests to the Linear API.
 *
 * @module LinearGraphQLClient
 */
import {
  LinearApiError,
  type LinearAssignLabelResult,
  type LinearIssue,
  type LinearLabel,
} from "@t3tools/contracts";
import { Effect } from "effect";
import * as HttpClient from "effect/unstable/http/HttpClient";
import * as HttpClientRequest from "effect/unstable/http/HttpClientRequest";
import * as HttpClientResponse from "effect/unstable/http/HttpClientResponse";

const LINEAR_API_URL = "https://api.linear.app/graphql";
const LINEAR_REQUEST_TIMEOUT = "30 seconds";

interface GraphQLResponse<T> {
  data?: T | undefined;
  errors?: readonly { message: string }[] | undefined;
}

const linearApiError = (detail: unknown) =>
  new LinearApiError({
    detail: detail instanceof Error ? detail.message : String(detail),
  });

function linearGraphQL<T>(
  token: string,
  query: string,
  variables?: Record<string, unknown>,
): Effect.Effect<T, LinearApiError, HttpClient.HttpClient> {
  return Effect.gen(function* () {
    const httpClient = yield* HttpClient.HttpClient;
    const json = yield* HttpClientRequest.post(LINEAR_API_URL).pipe(
      HttpClientRequest.setHeader("Authorization", token),
      HttpClientRequest.bodyJson({ query, variables }),
      Effect.flatMap(httpClient.execute),
      Effect.flatMap(HttpClientResponse.filterStatusOk),
      Effect.flatMap((response) => response.json),
      Effect.timeout(LINEAR_REQUEST_TIMEOUT),
      Effect.mapError(linearApiError),
    );

    const body = json as GraphQLResponse<T>;
    if (body.errors && body.errors.length > 0) {
      return yield* Effect.fail(linearApiError(body.errors.map((e) => e.message).join(", ")));
    }
    if (!body.data) {
      return yield* Effect.fail(linearApiError("No data in GraphQL response"));
    }
    return body.data;
  });
}

// ── Queries ───────────────────────────────────────────────────────────

const ASSIGNED_ISSUES_QUERY = `
query AssignedIssues($after: String) {
  viewer {
    assignedIssues(
      filter: { state: { type: { in: ["backlog", "unstarted", "started"] } } }
      first: 100
      after: $after
      orderBy: updatedAt
    ) {
      pageInfo { hasNextPage endCursor }
      nodes {
        id
        identifier
        title
        priority
        priorityLabel
        url
        createdAt
        updatedAt
        state { id name color type }
        project { id name color }
        cycle { id name number startsAt endsAt }
        labels { nodes { id name color } }
      }
    }
  }
}
`;

interface AssignedIssuesResponse {
  viewer: {
    assignedIssues: {
      pageInfo: { hasNextPage: boolean; endCursor: string | null };
      nodes: readonly RawLinearIssue[];
    };
  };
}

interface RawLinearIssue {
  id: string;
  identifier: string;
  title: string;
  priority: number;
  priorityLabel: string;
  url: string;
  createdAt: string;
  updatedAt: string;
  state: { id: string; name: string; color: string; type: string };
  project: { id: string; name: string; color: string } | null;
  cycle: {
    id: string;
    name: string | null;
    number: number;
    startsAt: string;
    endsAt: string;
  } | null;
  labels: { nodes: readonly { id: string; name: string; color: string }[] };
}

function mapRawIssue(raw: RawLinearIssue): LinearIssue {
  return {
    id: raw.id,
    identifier: raw.identifier,
    title: raw.title,
    priority: raw.priority,
    priorityLabel: raw.priorityLabel,
    url: raw.url,
    createdAt: raw.createdAt,
    updatedAt: raw.updatedAt,
    state: raw.state,
    project: raw.project,
    cycle: raw.cycle,
    labels: [...raw.labels.nodes],
  };
}

export function fetchAssignedIssues(
  token: string,
): Effect.Effect<LinearIssue[], LinearApiError, HttpClient.HttpClient> {
  return Effect.gen(function* () {
    const allIssues: LinearIssue[] = [];
    let after: string | null = null;
    let hasNextPage = true;

    while (hasNextPage) {
      const data: AssignedIssuesResponse = yield* linearGraphQL<AssignedIssuesResponse>(
        token,
        ASSIGNED_ISSUES_QUERY,
        { after },
      );
      const connection: AssignedIssuesResponse["viewer"]["assignedIssues"] =
        data.viewer.assignedIssues;
      for (const raw of connection.nodes) {
        allIssues.push(mapRawIssue(raw));
      }
      hasNextPage = connection.pageInfo.hasNextPage;
      after = connection.pageInfo.endCursor;
    }

    return allIssues;
  });
}

// ── Labels query ─────────────────────────────────────────────────────

const LABELS_QUERY = `
query IssueLabels {
  issueLabels(first: 100) {
    nodes { id name color }
  }
}
`;

interface LabelsResponse {
  issueLabels: {
    nodes: readonly { id: string; name: string; color: string }[];
  };
}

export function fetchLabels(
  token: string,
): Effect.Effect<LinearLabel[], LinearApiError, HttpClient.HttpClient> {
  return Effect.gen(function* () {
    const data = yield* linearGraphQL<LabelsResponse>(token, LABELS_QUERY);
    return [...data.issueLabels.nodes];
  });
}

// ── Assign label mutation ────────────────────────────────────────────

const ASSIGN_LABEL_MUTATION = `
mutation IssueAddLabel($issueId: String!, $labelId: String!) {
  issueAddLabel(id: $issueId, labelId: $labelId) {
    success
    issue {
      id
      identifier
      labels { nodes { id name color } }
    }
  }
}
`;

interface AssignLabelResponse {
  issueAddLabel: {
    success: boolean;
    issue: {
      id: string;
      identifier: string;
      labels: { nodes: readonly { id: string; name: string; color: string }[] };
    };
  };
}

export function assignIssueLabel(
  token: string,
  issueId: string,
  labelId: string,
): Effect.Effect<LinearAssignLabelResult, LinearApiError, HttpClient.HttpClient> {
  return Effect.gen(function* () {
    const data = yield* linearGraphQL<AssignLabelResponse>(token, ASSIGN_LABEL_MUTATION, {
      issueId,
      labelId,
    });

    if (!data.issueAddLabel.success) {
      return yield* new LinearApiError({ detail: "Failed to assign label" });
    }

    return {
      issueId: data.issueAddLabel.issue.id,
      identifier: data.issueAddLabel.issue.identifier,
      labels: [...data.issueAddLabel.issue.labels.nodes],
    };
  });
}
