# Billing (OSS MVP)

Platform administrators can manage token usage, period statements, attribution,
prices, token budgets and rate limits under **Usage → Billing**. The UI starts in
Token mode on every visit. The Money switch changes the view on the same page.

## Enable and upgrade

Deploy the OSS billing backend branch together with the billing UI. Back up the
database first. Start the upgraded server using the deployment's existing
configuration: the normal server startup runs Alembic migrations. Confirm the
revision reaches `e1b6c2d4a8f0` and that the `system_settings`, `quota_rules`,
and `quota_usage_samples` tables exist. These separately ordered revisions
follow upstream's `c8d3e6f1a902`; none of them requires audit-log or other
enterprise WIP migrations.
PostgreSQL/openGauss and MySQL/OceanBase use portable string columns for quota
kinds and subjects, avoiding native enum differences.

An older backend continues to supply token quantities through `/v2/usage/breakdown`.
The UI displays an upgrade banner and does not pretend to save settings or budgets.
Model grouping on that fallback represents model routes rather than deployments.

## Prices and reports

`GET` / `PUT /v2/billing/price-list` read and replace the validated price list:

```json
{
  "currency": "CNY",
  "token_to_money": { "unit_tokens": 1000000, "amount": 10 },
  "token_prices": {
    "qwen3": { "input_per_million": 5, "output_per_million": 15 }
  },
  "gpu_hour_prices": { "default": 4.5 }
}
```

Token prices resolve by exact model name, then `default`, then the simple
conversion. An explicit zero rate means free. GPU prices resolve by SKU, then
`default`, then zero. Currency codes must be three uppercase letters; rates must
be finite and nonnegative. There is no exchange-rate service. The fresh-install
conversion is zero until the operator configures a rate.

`GET /v2/billing/summary?start=YYYY-MM-DD&end=YYYY-MM-DD` returns prompt,
completion and total tokens, requests, token costs, GPU-hours and GPU costs.
`GET /v2/billing/token-breakdown` accepts the same dates and
`group_by=organization|user|model|api_key`. Organization means the consumer
principal and can include personal scopes. Identities remain separate even when
display names collide. GPU charges appear only in the period summary.

Dates include both endpoints, using UTC metering buckets. Prices are applied at
read time to all history. Exported CSV period statements include the loaded
breakdown and summary; they are reconciliation reports, not tax invoices or an
immutable ledger. Keep exported statements if a rate snapshot is needed.

## Quotas and alerts

Admin CRUD: `GET` / `POST /v2/quota-rules` and `GET` / `PUT` / `DELETE
`/v2/quota-rules/{id}`. PUT replaces the rule's editable fields. Supported kinds:
`token_budget`, `token_rate`, `request_rate`. Subjects are `global` (null ID),
`organization`, `user`, `model`, or `api_key` (positive numeric ID). Token rules use
tokens; request rules use requests, including in Money view. Windows are trailing
seconds, from 1 to 31,536,000; 2,592,000 means 30 days, not a calendar month.

Request admission is atomic across applicable rules. Exhausted rules return 429
with `Retry-After`. Token usage accrues after the usage collector commits its
batch, so concurrent/in-flight requests can exceed a token cap. Budget thresholds
at 80%, 95% and 100% produce persisted resource events, available through
`GET /v2/quota-rules/alerts` (latest 100).

`used_value` and `remaining_value` (`max(0, limit_value - used_value)`) are the
trailing window, reloaded from SQL after a restart. They are stored in
`quota_usage_samples` as one coalesced row per counter per UTC second. The
counter key is `quota:{rule_id}:{subject}` (`global` when the rule has no
subject id). The server keeps a copy of that window in memory and writes it
through:

- Request admission records `request_rate` before the request is allowed. A
  denial records nothing.
- `token_rate` and `token_budget` are recorded when usage accrual runs, including
  totals above the limit.
- Rows older than the rule's window are deleted on those writes. After a restart
  on the same database the server loads the remaining rows and keeps sliding
  them by wall-clock age. Windows stay trailing seconds; they are not calendar
  days and they are not pinned to a timezone.

Creating a rule does not clear other counters. Updating or deleting a rule
deletes that rule's samples, so its `used_value` returns to zero. Disabling a
rule is an update and clears its usage too.

**What a restart does not preserve, and what is still single-process:**

- Which 80/95/100 alerts were already emitted. That dedupe lives in process
  memory, so the next accrual after a restart can write the same threshold
  again while usage is still over it. The events already stored in
  `resource_events` remain.
- A sample whose SQL commit failed or did not finish. While the process is
  still up it keeps that sample and writes it on the next successful mutation
  of the same counter. If the process dies before the commit, a restart does
  not see that sample.
- Admission before the first successful load of `quota_usage_samples`. The
  server refuses the request instead of treating an empty memory window as
  zero usage. Accrual in that state is skipped so it cannot overwrite a
  second that SQL already holds.
- Agreement across multiple server processes. Each process loads the rows at
  first use and then writes its own copy of a second's amount. Two processes
  can both admit, and the later write replaces that second. These caps are not
  a distributed lock. Run one server process.
- A hard financial limit on in-flight token usage. Tokens are known only after
  the collector commits, so concurrent requests can still pass a token cap.
  The overage is what the next request is denied against, and it is kept.
- Wall-clock steps. A forward jump expires samples early; a backward jump keeps
  them until real time catches up.

A busy counter holds about one row per second for the length of its window. A
30-day budget under continuous traffic can reach about 2.6 million rows for
that counter. Quiet counters stay small because expired seconds are deleted.
This is not a settlement ledger; historical token quantities remain in the
usage tables, and prices are still applied at read time.

Direct server inference
(`/v1`, legacy alias and rerank) is guarded after model resolution, except internal
benchmarks. For gateway inference, the admission guard runs at `/token-auth`;
configure the existing `gateway_plugin` setting as follows and restart/reconcile
the gateway configuration:

```yaml
gateway_plugin:
  gpustack-ext-auth:
    config:
      local_auth:
        enabled: false
      failure_mode_allow_authenticated: false
```

Gateway PUBLIC routes that skip remote authorization are outside this guard.
Use authenticated routes for capped workloads. Model rules on a multi-target
gateway route conservatively gate all candidate model IDs. Token usage from all
collector paths still contributes to matching budgets. The server does not accept
an unvalidated client organization header as the quota identity: API-key ownership
and the authenticated tenant context determine it. Other processes observe rule
*definitions* within ten seconds and price edits within five seconds. They do
not share a live usage counter; see the single-process limit above.
