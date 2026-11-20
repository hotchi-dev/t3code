import { expect, it } from "@effect/vitest";
import {
  DEFAULT_SERVER_SETTINGS,
  EnvironmentId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  UsageLimitSourceId,
  type OrchestrationV2ThreadShell,
  type UsageLimitSourceSnapshot,
} from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";

import * as Environment from "../../../environment/ServerEnvironment.ts";
import * as ThreadCommandExecutor from "../../../orchestration-v2/ThreadCommandExecutor.ts";
import * as ThreadManagement from "../../../orchestration-v2/ThreadManagementService.ts";
import * as Settings from "../../../serverSettings.ts";
import * as UsageLimitSources from "../../../usage/UsageLimitSources.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import * as McpToolAccess from "../../McpToolAccess.ts";
import { liveThreadShell } from "../../McpToolAccess.testkit.ts";
import * as EnvironmentHandlers from "./handlers.ts";
import { EnvironmentToolkit } from "./tools.ts";

const environmentId = EnvironmentId.make("environment:preferences");
const threadId = ThreadId.make("thread:preferences");

it.effect("returns cached hub quotas for routing without account metadata or probe errors", () =>
  Effect.gen(function* () {
    const toolkit = yield* EnvironmentToolkit;
    const result = yield* toolkit
      .handle("t3_vibeproxy_status", {})
      .pipe(Stream.unwrap, Stream.runCollect);
    expect(result.at(-1)?.result).toEqual({
      sources: [
        {
          id: "hub",
          label: "VibeProxy",
          checkedAt,
          status: "ready",
          accounts: [
            {
              driver: "claudeAgent",
              checkedAt: accountCheckedAt,
              status: "ready",
              windows: [
                {
                  kind: "weekly",
                  label: "Fable",
                  modelIds: ["claude-fable"],
                  usedPercent: 100,
                  resetsAt: "2026-11-24T09:00:00.000Z",
                },
                { kind: "session", label: "Session", usedPercent: 0, windowDurationMins: 300 },
              ],
            },
            { driver: "codex", checkedAt: accountCheckedAt, status: "unavailable", windows: [] },
          ],
        },
        {
          id: "offline",
          label: "Offline hub",
          checkedAt,
          status: "unavailable",
          accounts: [],
        },
        { id: "empty", label: "Empty hub", checkedAt, status: "no_accounts", accounts: [] },
      ],
    });
  }).pipe(
    Effect.provide(McpToolAccess.HandlersLayer.layer(EnvironmentHandlers.layer)),
    Effect.provideService(McpInvocationContext.McpInvocationContext, {
      environmentId,
      requestNamespace: "client:routing",
      thread: undefined,
      client: { sessionId: "routing", label: "Routing client", access: "read-only" },
      capabilities: new Set(["orchestration" as const]),
      issuedAt: 0,
    }),
    Effect.provide(
      Layer.mock(UsageLimitSources.UsageLimitSources)({ current: Effect.succeed(sources) }),
    ),
  ),
);

const checkedAt = "2026-11-20T09:00:00.000Z";
const accountCheckedAt = "2026-11-20T09:00:01.000Z";
const sources: ReadonlyArray<UsageLimitSourceSnapshot> = [
  {
    id: UsageLimitSourceId.make("hub"),
    kind: "cliproxy",
    label: "VibeProxy",
    checkedAt,
    accounts: [
      {
        id: "claude-account.json",
        email: "developer@example.test",
        workspaceId: "workspace-id",
        driver: ProviderDriverKind.make("claudeAgent"),
        usageLimits: {
          checkedAt: accountCheckedAt,
          credentialFingerprint: "credential-fingerprint",
          resetCredits: { availableCount: 1, nextCreditId: "reset-credit-id" },
          windows: [
            {
              id: "fable-weekly",
              kind: "weekly",
              label: "Fable",
              modelIds: ["claude-fable"],
              usedPercent: 100,
              resetsAt: "2026-11-24T09:00:00.000Z",
            },
            {
              id: "five-hour",
              kind: "session",
              label: "Session",
              usedPercent: 0,
              windowDurationMins: 300,
            },
          ],
        },
      },
      {
        id: "codex-account.json",
        driver: ProviderDriverKind.make("codex"),
        usageLimits: {
          checkedAt: accountCheckedAt,
          unavailable: { reason: "probeFailed", message: "private upstream details" },
          windows: [{ id: "weekly", kind: "weekly", label: "Weekly", usedPercent: 20 }],
        },
      },
    ],
  },
  {
    id: UsageLimitSourceId.make("offline"),
    kind: "cliproxy",
    label: "Offline hub",
    checkedAt,
    error: "private source error",
    accounts: [],
  },
  {
    id: UsageLimitSourceId.make("empty"),
    kind: "cliproxy",
    label: "Empty hub",
    checkedAt,
    accounts: [],
  },
];

it.effect("refuses a preferences update when the caller's turn ends while it waits", () =>
  Effect.gen(function* () {
    const caller = yield* Ref.make<OrchestrationV2ThreadShell>(liveThreadShell(threadId));
    const updates = yield* Ref.make(0);
    const checked = yield* Deferred.make<void>();
    const layerDependencies = Layer.mergeAll(
      ThreadCommandExecutor.layer,
      Layer.succeed(McpInvocationContext.McpInvocationContext, {
        environmentId,
        requestNamespace: "provider:preferences",
        thread: {
          threadId,
          providerSessionId: "provider:preferences",
          providerInstanceId: ProviderInstanceId.make("codex"),
        },
        client: undefined,
        capabilities: new Set(["orchestration" as const]),
        issuedAt: 0,
      }),
      Layer.mock(ThreadManagement.ThreadManagementService)({
        getThreadShell: () =>
          Ref.get(caller).pipe(Effect.tap(() => Deferred.succeed(checked, undefined))),
      }),
      Layer.mock(Environment.ServerEnvironment)({
        getDescriptor: Effect.succeed({
          environmentId,
          label: "Test",
          platform: { os: "linux", arch: "x64" },
          serverVersion: "0.0.0",
          capabilities: { repositoryIdentity: false },
        }),
      }),
      Layer.mock(Settings.ServerSettingsService)({
        getSettings: Effect.succeed(DEFAULT_SERVER_SETTINGS),
        updateSettings: () =>
          Ref.update(updates, (count) => count + 1).pipe(Effect.as(DEFAULT_SERVER_SETTINGS)),
      }),
    );
    yield* Effect.gen(function* () {
      const toolkit = yield* EnvironmentToolkit;
      const executor = yield* ThreadCommandExecutor.ThreadCommandExecutor;
      const held = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const holder = yield* executor
        .withLock(
          threadId,
          Deferred.succeed(held, undefined).pipe(Effect.andThen(Deferred.await(release))),
        )
        .pipe(Effect.forkChild);
      yield* Deferred.await(held);
      const update = yield* toolkit
        .handle("t3_environment_preferences_update", { newWorktreesStartFromOrigin: true })
        .pipe(Stream.unwrap, Stream.runCollect, Effect.forkChild);
      yield* Deferred.await(checked);
      yield* Ref.update(caller, (shell) => ({ ...shell, activeRunId: null }));
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.join(holder);
      const result = yield* Fiber.join(update);
      expect(result.at(-1)?.result).toMatchObject({ code: "parent_not_active" });
      expect(yield* Ref.get(updates)).toBe(0);
    }).pipe(
      Effect.provide(
        McpToolAccess.HandlersLayer.layer(EnvironmentHandlers.layer).pipe(
          Layer.provideMerge(layerDependencies),
        ),
      ),
    );
  }),
);
