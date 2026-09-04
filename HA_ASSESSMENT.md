# Drone CI Server: High-Availability Readiness Assessment

## Context

The Drone server (`github.com/drone/drone`, this fork at `rhiaxion/gitness`) runs today as a
single instance. That makes CI a single point of failure: every restart, node failure, or
deploy is a full outage, and there is no way to scale horizontally.

This document assesses what would be required to run N instances active/active behind a load
balancer. It is an analysis deliverable — **no code changes are proposed for immediate
implementation.** Every claim below was verified by reading the source; file and line
references are included so each can be checked independently.

**Production context** (from `DATABASE_PERFORMANCE_PLAN.md` on this branch): production runs
Postgres on Cloud SQL (`drone-db-eda4bbbd`, project `cicd-corp-001`). That matters — it means
the hardest prerequisite is already satisfied.

---

## Verdict

**HA is substantially closer than the single-instance deployment suggests, but it is not a
configuration-only change.**

Drone already ships Redis-backed implementations of the three subsystems that hold
cross-request state, and they activate automatically when Redis is configured. Sessions are
already stateless, and the build-dispatch path already has a database-level correctness
backstop.

What remains is a short list of genuine gaps. Two of them are blocking:

1. **Concurrent schema migrations race on startup** — multi-instance rollout crash-loops.
2. **The cron scheduler fires every scheduled build N times** — no amount of Redis fixes this.

Both are small, well-understood fixes. Beyond those, one larger issue (cross-instance
concurrency-limit enforcement) is a design question rather than a bug fix, and is the main
reason to treat this as a project rather than a config change.

**Rough shape of the work:** two small fixes to unblock, four more small fixes to remove
operational sharp edges, and one scoped piece of design work if strict `concurrency:`
enforcement is a requirement.

---

## Part 1 — What already works

No code changes needed for any of this. Verified by reading each implementation.

| Concern | Redis-backed implementation | Selection logic |
|---|---|---|
| UI event stream / build feed | `pubsub/hub_redis.go` (channel `drone-events`) | `pubsub/pubsub.go` — `newHubRedis(r)` when `r != nil` |
| Live log tailing | `livelog/stream_redis.go` (Redis Streams, keys `drone-log-<stepID>`, 5h TTL) | `livelog/livelog.go` — `newStreamRedis(rdb)` when `rdb != nil` |
| Build cancellation fan-out | `scheduler/queue/canceller_redis.go` (channel `drone-cancel` + `drone-cancel-<id>` key, 5m TTL) | `scheduler/queue/scheduler_non_oss.go` |
| Queue dispatch serialization | `redsync` mutex `drone-scheduler-mx`, `scheduler/queue/scheduler_non_oss.go:41-44` | same |

Three further properties already hold:

**Sessions are stateless.** `session/session.go` uses `dchest/authcookie` — an HMAC-signed
`login|expiry|signature` cookie, verified with a shared secret, followed by a DB lookup.
There is no server-side session map. Any instance can validate any session (subject to the
secret caveat in Part 2, which is important).

**Stage assignment has a database-level correctness backstop.** `operator/manager/manager.go:205`
(`Accept`) checks `stage.Machine != ""` and relies on compare-and-swap via the `Version`
column (`store/stage/stage.go:355-362`: `AND stage_version = :stage_version_old`). Its own
comment states the intent plainly: *"It is possible for multiple agents to pull the same stage
from the queue. The system uses optimistic locking at the database-level to prevent multiple
agents from executing the same stage."* The losing runner discards the item and re-polls
(`operator/runner/runner.go:586`). So **two instances cannot execute the same stage** — that
core safety property is already guaranteed.

**The alarming-looking in-process DB mutex is already inert on a real database.**
`store/shared/db/db.go:88` `Lock()` is used by nearly every write path and is documented as
"(sqlite only)". `store/shared/db/conn.go:41-51` installs `nopLocker{}` for both `postgres`
and `mysql`, reserving the real `sync.RWMutex` for sqlite. On production Postgres it is a
pass-through. This is correct design, not a gap — but note the corollary: **it provides zero
cross-instance protection, so any read-modify-write that relied on it is unprotected.** That
is the root of gaps #2 and #7 below.

---

## Part 2 — Required configuration

These are prerequisites, not fixes. Each one fails *silently* if missed, which is what makes
them worth enumerating.

**Build without the `oss` tag.** Under `-tags oss` all three Redis implementations are
compiled out and replaced with in-memory stubs (`pubsub/pubsub_oss.go`, `livelog/livelog_oss.go`,
`scheduler/queue/scheduler_oss.go`); `store/shared/db/conn_oss.go` also drops Postgres and
MySQL support entirely. HA is impossible in that build. The `Taskfile.yml` `build-base` target
passes no tags, so the shipping build is already the correct one — just don't change it.

**`DRONE_COOKIE_SECRET` must be set explicitly and identically on every instance.** This is
the sharpest edge in the whole assessment. `cmd/drone-server/config/config.go:574`:

```go
func defaultSession(c *Config) {
	if c.Session.Secret == "" {
		c.Session.Secret = uniuri.NewLen(32)
	}
}
```

If unset, **each instance generates its own random secret at boot.** Cookies minted by
instance A are rejected by instance B, so users are bounced to the login page on whichever
requests happen to land elsewhere — intermittently, with no error logged anywhere. (This also
means single-instance deployments today log everyone out on every restart, which is a useful
way to check whether it is currently set.)

**Redis must be provisioned, and its absence is silent.** `service/redisdb/redisdb.go` returns
`(nil, nil)` when neither `DRONE_REDIS_CONNECTION` nor `DRONE_REDIS_ADDR` is set — no error, no
warning. Every consumer branches on `if r != nil` and quietly falls back to in-memory. It is
therefore entirely possible to believe Redis is active when it is not. **Recommendation: add a
startup assertion that fails fast when multi-instance operation is intended but Redis is
absent.** Since there is no instance-count config today, the simplest form is a new explicit
opt-in flag whose presence requires a non-nil Redis client.

What breaks if Redis is missed, concretely:
- *Live log tailing shows immediate EOF* whenever the LB routes the viewer to an instance not
  running the step. `handler/api/events/logs.go:99-103` treats a nil error channel as
  end-of-stream and gives up. Completed logs still work — they are persisted — so this
  presents as "live tailing is broken about half the time."
- *The UI stops live-updating* for builds processed by another instance (`pubsub/hub.go` is a
  local subscriber map).
- *Cancellation degrades rather than hangs* — `Manager.Watch` (`operator/manager/manager.go:515`)
  falls back to re-reading the build from the DB, so a cancel lands within ~1 minute instead of
  near-instantly.

**Other config that must be identical across instances:** `DRONE_RPC_SECRET` (runner auth),
`DRONE_DATABASE_SECRET` (AES key for secret encryption — divergence makes secrets
undecryptable), and the behavior-affecting filters `DRONE_REPOSITORY_FILTER`,
`DRONE_USER_FILTER`, `DRONE_REGISTRATION_CLOSED`. Divergence in the last group produces
behavior that varies by which instance serves the request.

**Database:** `DRONE_DATABASE_DRIVER=postgres` with a shared datasource. Already true in
production. Note the default is `sqlite3` / `core.sqlite` (`config.go:118-119`) and the Docker
image defaults to a local volume (`docker/Dockerfile.server.linux.amd64:14-15`) — sqlite cannot
be shared, and there is no WAL or `busy_timeout` configuration anywhere in the tree, so its
serialization is purely the in-process mutex.

**Keep remote agents enabled.** `cmd/drone-server/inject_runner.go` instantiates an embedded
Docker runner only when `config.Agent.Disabled == true`. If agents are disabled, **each server
instance runs its own runner**, so effective build capacity silently becomes
`N × DRONE_RUNNER_CAPACITY` and every instance needs a Docker socket.

**Externalize logs** to `DRONE_S3_BUCKET` or Azure Blob (`store/logs/s3.go`,
`store/logs/azureblob.go`, selected in `inject_store.go`). Otherwise logs live in the database
(`store/logs/logs.go`) — which is shared and therefore *correct*, just a heavier load path.
`store/logs/combine.go` supports a DB→S3 migration.

---

## Part 3 — Code gaps, ranked

### 1. Schema migrations race on startup — *blocking*

`store/shared/migrate/postgres/ddl_gen.go:211` — `Migrate()` is a check-then-act loop:
`selectCompleted(db)`, then for each pending migration `db.Exec(migration.stmt)` followed by
`insertMigration(db, name)`. There is no advisory lock, no per-migration transaction, and no
migration-table lock. It runs from `setupDatabase()` (`store/shared/db/conn.go`) on **every**
server startup.

When N instances boot together — a rolling deploy, or a fresh cluster — they all observe the
same migration as pending and all execute its DDL. The losers fail on either the DDL itself
(e.g. duplicate `ADD COLUMN`) or the `UNIQUE(name)` constraint on the `migrations` table.
`main.go` treats an initialization error as fatal (`logger.Fatalln("main: cannot initialize server")`),
so **the losing instances crash-loop.**

This is invisible with one instance and blocks the very first multi-instance rollout.

*Fix shape:* wrap the migration run in a Postgres advisory lock (`pg_advisory_lock` /
`pg_advisory_unlock`, or `pg_try_advisory_lock` with a wait loop) so exactly one instance
migrates while the others block until it completes. MySQL's equivalent is `GET_LOCK()`. Small,
self-contained, and independently testable. Note there is currently no SQL-level locking
anywhere in the codebase — no `FOR UPDATE`, no advisory locks — so this introduces a new
pattern.

### 2. Cron scheduler fires every scheduled build N times — *blocking*

`trigger/cron/cron.go:48` — `Start()` is a bare `time.Ticker`, started in every instance from
`main.go`. `run()` calls `s.cron.Ready(ctx, now.Unix())` (which is `WHERE cron_next < :cron_next`)
and then `s.cron.Update(ctx, job)` as two separate statements. N instances all see the same job
as due, all succeed at the update, and all call `s.trigger.Trigger(...)`.

**Result: every cron pipeline fires N times per interval.** This is the only gap that produces
visibly wrong user-facing behavior, and it cannot be fixed by configuring Redis.

The notable part is that **the fix is a small SQL change, because the version column already
exists but is unused.** The `cron` table has `cron_version`; it is selected
(`store/cron/cron.go:162`), inserted (`:238`), and written on update (`:203`) — but
`stmtUpdate`'s WHERE clause is `WHERE cron_id = :cron_id` with no version predicate, and
`cronStore.Update` (`:124`) binds the version as-is without incrementing it. So the
compare-and-swap infrastructure is in place and simply not wired up. Compare
`store/stage/stage.go:355-362`, which does it correctly.

*Fix shape:* bind `:cron_version_old` / `:cron_version_new`, add `AND cron_version = :cron_version_old`
to the WHERE, check `RowsAffected() == 0` and return `db.ErrOptimisticLock` — mirroring the
established stage-store pattern. Then in `trigger/cron/cron.go`, treat that error as "another
instance claimed this job" and skip **before** triggering. Do not rely on `db.Lock()` here; it
is a no-op on Postgres.

Optionally also wrap the tick in the distributed mutex to avoid N instances doing redundant SCM
lookups, but the compare-and-swap is the load-bearing change and is sufficient on its own.

*Interim mitigation available today without code changes:* set `DRONE_CRON_DISABLED=true` on
all but one instance. This works but is fragile — it makes instances non-identical and
reintroduces a single point of failure for cron.

### 3. Concurrency limits can be exceeded across instances — *high; design work, not a bug fix*

`scheduler/queue/queue.go` — `withinLimits` (`:278`) and `shouldThrottle` (`:301`) both reason
over the `items` slice *this instance* fetched from `q.store.ListIncomplete(ctx)`. Two instances
can independently conclude the same stage is within `stage.Limit` (the per-pipeline
`concurrency:` setting) or under `item.LimitRepo`, and both dispatch.

The `redsync` mutex does **not** close this, because `signal()` writes nothing to the store —
the reservation update that would have made this safe is commented-out dead code at
`scheduler/queue/queue.go:202-214` ("the queue has 60 seconds to ack the item"). The lock is
released as soon as work is handed to a local channel, well before `Manager.Accept` persists
`stage.Machine`, so the window between "A decided to dispatch" and "the database reflects it"
is wide open and B's snapshot cannot see A's intent.

**Consequence: `concurrency:` in `.drone.yml` and `DRONE_LIMIT_*` can be exceeded, up to roughly
N× the configured limit.** For anyone using `concurrency: 1` to serialize deployments, this is a
correctness hazard rather than an inefficiency — and it is *not* fixed by enabling Redis.

Note this does not compromise the core guarantee from Part 1: the same stage still cannot run
twice, because `Accept` is version-guarded. What can happen is that *more distinct stages* run
concurrently than the limit permits.

*Options, in increasing order of effort:*
- **Accept and document** the limit as best-effort. Viable if no pipeline depends on
  `concurrency` for correctness — worth auditing actual `.drone.yml` usage before deciding.
- **Run dispatch on one designated instance** while serving HTTP from all of them. Preserves
  HA for the web tier and for read traffic, which is most of the value, and sidesteps the
  problem. Requires a way to designate that instance.
- **Restore the reservation step:** inside the mutex, write a claim to the stage row so a
  later instance's `ListIncomplete` snapshot observes it. The existing `Machine != ""` skip in
  `signal()` already handles the read side, so a reservation write makes the existing filter
  effective. This is the largest piece of work here and touches the dispatch hot path.

### 4. Queue pause/resume only affects one instance — *medium*

`scheduler/queue/queue.go:63-84` stores `paused` as a plain in-process `bool` behind a local
`sync.Mutex`. The admin endpoints (`handler/api/queue/pause.go`, `resume.go`, routed at
`handler/api/api.go:320-321`) therefore affect only whichever instance the load balancer
picked. `Paused()` likewise reports local state.

This matters operationally: an engineer pausing the queue during an incident will believe the
queue is stopped while other instances keep dispatching. Because `schedulerRedis` embeds
`*queue` (`scheduler/queue/scheduler_redis.go:24-27`), **this is broken even with Redis
configured.**

*Fix shape:* move the flag to shared state — a Redis key consulted by `signal()`, or a row in
the database.

### 5. Zombie reaper duplicates work — *medium*

`service/canceler/reaper/reaper.go:75` — the same unguarded-ticker pattern, started in every
instance. N instances concurrently enumerate pending/running builds and call
`r.Canceler.Cancel(...)` on the same ones.

Lower severity than cron because the underlying `builds.Update` is version-guarded, so losers
get `ErrOptimisticLock`. Expect log noise and possibly duplicate webhooks / status posts (the
webhook and pubsub publish in `service/canceler/canceler.go` happen after the guarded update)
rather than corruption. Default interval is 24h, so any lock must be taken per tick, not held
across the sleep.

### 6. Datadog metrics double-report — *low*

`metric/sink/datadog.go:68` — a once-daily aggregation (`midnightDiff()`) started in every
instance. N instances report N times, inflating counts. Guard it, or enable it on one instance.

Separately, the Prometheus gauges in `metric/{builds,stages,repos,users}.go` query the database
so their *values* agree across instances, but `/metrics` must be scraped per-instance and
aggregated appropriately.

### 7. OAuth token refresh is last-write-wins — *low probability, high blast radius*

`store/user/user.go:271` — `stmtUpdate` has **no** version guard (`WHERE user_id = :user_id`),
unlike the repo, stage, step, and build stores. It writes `user_oauth_token`,
`user_oauth_refresh`, and `user_oauth_expiry`.

`service/token/renew.go:45-64` `Renew()` checks expiry, calls `r.refresh.Refresh(t)`, then
`r.users.Update(ctx, user)`. Under N instances, two concurrent requests for the same user can
both detect expiry and both hit the SCM refresh endpoint. For providers that rotate refresh
tokens on use (GitLab, Bitbucket), the second refresh invalidates the first, and last-write-wins
may persist the now-dead token — breaking that user's SCM integration until they re-login.

Narrow race window, but the failure is confusing to diagnose and user-visible. The fix follows
the established pattern: add a `user_version` column and compare-and-swap, as
`store/repos/repos.go:209-236` already does.

### 8. Admin observability degrades — *low*

- `scheduler/queue/scheduler_redis.go:29` — `Stats()` returns `errors.New("not implemented")`.
  Enabling Redis breaks the queue-stats admin surface, precisely when a cluster makes it most
  useful.
- `handler/api/system/stats.go` — `Events.Subscribers` and `Streams` come from
  `Pubsub.Subscribers()` / `LogStream.Info()`, which count only *local* in-process subscribers
  even in the Redis implementations (`pubsub/hub_redis.go:93`, `livelog/stream_redis.go:185`).
  Numbers become per-instance and misleading.

### 9. Per-instance cache staleness — *accept and document*

Four `hashicorp/golang-lru` caches, none shared or invalidated across instances:

| Path | Size | TTL | Staleness impact |
|---|---|---|---|
| `service/org/cache.go:37` | 25 | per-item, from config | **Highest** — caches SCM org membership and admin flag. A revoked membership stays honored on other instances until their copy expires. |
| `service/content/cache/contents.go:27` | 25 | none | Keyed `repo/commit/path` — content-addressed, so effectively immune. |
| `plugin/config/memoize.go:40` | 10 | none | Key includes `Repo.ID` + `Build.Created` — per-build, low risk. |
| `plugin/converter/memoize.go:40` | 10 | none | Same. |

Only the org cache carries real risk, and it is TTL-bounded. Recommend documenting rather than
changing. Two incidental observations while reading these: `service/org/cache.go` ignores the
`size` parameter passed to `NewCache` and hardcodes 25, and its `mu sync.Mutex` (line 48) is
declared but never used. Both are pre-existing and unrelated to HA.

### 10. SSE authorization snapshot — *pre-existing, worth noting*

`handler/api/events/global.go:48-54` — `HandleGlobal` snapshots the user's repo ACL once at
connection open and filters against that map for the connection's lifetime, which is capped at
24h. Revoked access continues to leak build events until reconnect. This is a single-instance
issue too, not caused by HA, but N instances multiply the number of long-lived stale-ACL
connections.

---

## Part 4 — Infrastructure considerations

**Redis becomes the new single point of failure.** `service/redisdb/redisdb.go` builds a plain
`redis.NewClient(options)` from a single `Addr` or connection URL. There is no
`NewFailoverClient` (Sentinel) and no `ClusterClient` support. Making the server HA while
leaving Redis standalone relocates the SPOF rather than removing it. Either add Sentinel/Cluster
support to `redisdb.New`, or use a managed HA Redis that presents one stable address — the
latter is simpler and avoids the `redsync` correctness questions that arise under failover
(single-node redsync is a lease, not a consensus lock, and its safety degrades if the primary
changes while a lock is held).

Because a lost Redis degrades live logs and the event stream but does *not* stop builds
(dispatch falls back to DB polling and `Accept` remains authoritative), a Redis outage is a
partial degradation rather than a full CI outage. Worth confirming that tradeoff is acceptable.

**Load balancer.** Sticky sessions are *not* required once Redis is on — any instance can serve
any log tail or event stream. The three SSE endpoints (`handler/api/events/`, routed at
`handler/api/api.go:352-360`) are long-lived: they ping every 30s and hard-close at 24h, so the
LB needs generous idle and read timeouts. Runner RPC is long-poll with a 30s timeout
(`operator/manager/rpc2/handler.go`). There are no websockets anywhere.

**Terminate TLS at the load balancer.** `server/server.go:161-167` caches ACME/Let's Encrypt
certificates on local disk (`$XDG_CACHE_HOME/golang-autocert` or `$HOME/.cache/golang-autocert`)
when `DRONE_TLS_AUTOCERT=true`. N instances would each solve their own ACME challenge against
the same hostname and cache separately — this will hit Let's Encrypt rate limits and cannot work
behind a single-hostname LB. This is the only local-disk state in the server; there is no
local-disk log or artifact store.

**Database capacity and contention.** `DRONE_DATABASE_MAX_CONNECTIONS` defaults to `0`
(unlimited) per instance, so N instances multiply the Cloud SQL connection count — set it
deliberately. More importantly, `DATABASE_PERFORMANCE_PLAN.md` already documents visible write
contention on this instance: 56.7s/week of lock time on the single-row `perms` insert and
**145s/week on a stage-update statement**. That second figure is the optimistic-lock path
(`store/stage/stage.go`), which is exactly what additional instances amplify — more dispatch
passes means more `Accept` collisions and more CAS retries. **Recommendation: land the read-side
work already on this branch, and re-measure that stage-update lock time, before adding
instances.** Postgres itself should also be HA (primary/standby) or the SPOF simply moves again.

**Migration ordering during rollout.** Once gap #1 is fixed, one instance migrates while others
wait. That still means schema changes must be backward-compatible with the *old* server version
during a rolling deploy, since old and new instances will briefly run against the same schema.
This is a new operational constraint that does not exist today.

---

## Part 5 — Recommended sequencing

Presented as a recommendation for a follow-up decision, not as work to start now.

**Tier 0 — unblocks a multi-instance rollout at all:**
1. Advisory lock around migrations (#1). Without it the first rollout crash-loops.
2. Cron compare-and-swap (#2). The only user-visible correctness bug. `DRONE_CRON_DISABLED` on
   all but one instance is a viable stopgap.

Plus the Part 2 configuration prerequisites — above all `DRONE_COOKIE_SECRET`, and a startup
assertion so a missing Redis fails loudly instead of silently.

**Tier 1 — removes operational sharp edges:**
3. Cluster-wide queue pause (#4) — operators will otherwise be misled during incidents.
4. Reaper guard (#5) and Datadog guard (#6).
5. User-table optimistic lock (#7).
6. Queue stats and system stats (#8).

A small shared helper — "run this periodic function under a named distributed lock, falling back
to a no-op when Redis is absent" — covers #5 and #6 and belongs alongside the existing
`redisdb.LockErr` / `LockErrNoOp` abstraction in `service/redisdb/`, which already provides
exactly the right interface. There is precedent for the pattern at `scheduler/queue/queue.go:30`.

**Tier 2 — decide before committing:**
7. Concurrency-limit enforcement (#3). Requires choosing among accept-as-best-effort,
   single-dispatcher, or the reservation rework. **This decision should come first**, since
   picking single-dispatcher would change the target architecture and make some Tier 1 items
   moot.

**Not recommended:** changing the LRU caches (#9), or the SSE ACL snapshot (#10), which is
pre-existing.

---

## Part 6 — How to validate

Since no code changes are proposed here, this section covers how to verify the assessment's
claims and what an eventual implementation would need to prove.

**Verify the claims in this document.** Every gap cites `file:line`. The two blocking ones are
worth confirming first, and both are cheap:
- Gap #1: read `store/shared/migrate/postgres/ddl_gen.go:211-234` and confirm there is no lock
  around the loop.
- Gap #2: compare `store/cron/cron.go:190-205` against `store/stage/stage.go:355-362` — the
  missing `AND cron_version = :cron_version_old` is visible side by side.

**Confirm current configuration**, since two prerequisites fail silently:
- Is `DRONE_COOKIE_SECRET` set in the production deployment? A quick proxy: do users stay logged
  in across a server restart? If not, it is unset.
- Is `DRONE_REDIS_ADDR` or `DRONE_REDIS_CONNECTION` set? If neither, Redis is not active
  regardless of whether one is deployed — `redisdb.New` returns nil silently.
- Is the shipping build produced without the `oss` tag? (`Taskfile.yml` suggests yes.)

**If implementation proceeds, the decisive tests would be:**

Both build configurations, since the `_oss.go` / `_non_oss.go` split is easy to break:
```
go build ./...
go build -tags "oss nolimit" ./...
go vet ./...
go test ./...
```

Postgres-backed store tests via the existing Taskfile target (note `DATABASE_PERFORMANCE_PLAN.md`
records that this has not yet been run against this branch's changes):
```
task test-postgres
```

*Migration race (#1)* — the decisive test. Against a fresh, empty Postgres database, launch
several instances simultaneously and confirm all reach a serving state with no
`cannot initialize server` fatals and exactly one row per migration in the `migrations` table.
This test fails reliably before the fix, which makes it a genuine before/after check.

*Cron duplication (#2)* — with two instances against shared Postgres and Redis, create a cron
entry and set a short `DRONE_CRON_INTERVAL`, then confirm exactly one build per period. Produces
one build *per instance* per period beforehand.

*Cluster-wide pause (#4)* — `DELETE /api/queue` against instance A, then confirm instance B also
stops dispatching (trigger a build, observe it stays pending), then `POST /api/queue` and confirm
it drains.

*Concurrency limits (#3)* — a pipeline with `concurrency: 1`, triggered repeatedly against two
instances, should never show two stages running at once.

*Failover smoke test* — two instances behind the LB: start a build, tail its logs in the UI, then
kill the instance serving the tail. The stream should reconnect through the other instance and the
build should continue, since both runner RPC and the log stream are backed by shared state rather
than the dead process.

*Database impact* — after any multi-instance change, re-check Cloud SQL Query Insights for lock
time on the stage-update statement and compare against the 145s/week baseline in
`DATABASE_PERFORMANCE_PLAN.md`.
