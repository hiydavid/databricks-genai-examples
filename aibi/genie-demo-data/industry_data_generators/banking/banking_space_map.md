# Bigly Bank Multi-Genie Domain Map

This is the implementation contract for the Bigly Bank synthetic dataset and
its focused Databricks Genie spaces. It defines schema ownership, notebook
boundaries, object names, generation order, and Genie data-source groups.
Implementation changes should follow this map; update the map first when a
design decision changes.

## Unity Catalog Layout

The implementation uses one user-supplied Unity Catalog and multiple schemas.
Both `catalog` and `schema_prefix` are user-supplied — set as defaults in the
orchestrator's Configuration cell or as notebook widgets — and neither is
hard-coded. Examples below use `bigly_bank` as a recommended schema prefix, but
the caller chooses both values at runtime.

| Alias | Runtime schema | Ownership boundary |
|---|---|---|
| CORE | `<catalog>.<schema_prefix>_core` | Shared conformed dimensions and relationships |
| RETAIL | `<catalog>.<schema_prefix>_retail` | Deposits, cards, and consumer lending |
| COMMERCIAL | `<catalog>.<schema_prefix>_commercial` | Small-business and commercial banking |
| WEALTH | `<catalog>.<schema_prefix>_wealth` | Wealth-management accounts and activity |
| OPERATIONS | `<catalog>.<schema_prefix>_operations` | Service and branch operations |
| RISK | `<catalog>.<schema_prefix>_risk` | Fraud, AML, KYC, alerts, and investigations |
| FINANCE | `<catalog>.<schema_prefix>_finance` | Finance and treasury aggregates |

For example, CORE resolves to `<catalog>.bigly_bank_core` when the supplied
prefix is `bigly_bank`. Schemas are business ownership boundaries, not Genie
space boundaries: Retail Deposits, Credit Cards, and Consumer Lending are three
Genie spaces backed by different subsets of the same RETAIL schema.

## Generator Organization Decision

Use one Databricks source notebook with one cell group per business domain. A
single Configuration cell at the top sets the shared `catalog`,
`schema_prefix`, `seed`, and `as_of_date`; every phase below reads those
globals. The separation boundary is the fact lifecycle, not every table and
not every Genie space.

```text
generate_banking_data.py   # Single notebook: Configuration + 11 phases
  Configuration            # catalog, schema_prefix, seed, as_of_date, enable_finance
  1  Shared Core           # parties, relationships, products, branches, employees, calendar
  2  Retail Deposits       # deposit/payment facts
  3  Credit Cards          # card facts
  4  Consumer Lending      # lending facts
  5  Commercial Banking    # COMMERCIAL facts
  6  Wealth Management     # WEALTH facts
  7  Service Operations    # OPERATIONS facts
  8  Fraud, AML & KYC      # RISK facts
  9  Finance & Treasury    # FINANCE facts (optional, enable_finance)
  10 Semantic layer        # Domain vw_ and mv_ objects
  11 Validation            # Cross-schema validation
```

Execution order is fixed:

1. CORE
2. RETAIL deposits, cards, and consumer lending
3. COMMERCIAL, WEALTH, and OPERATIONS
4. RISK, after transaction-producing domains
5. FINANCE, after all balance-producing domains, when enabled
6. Curated `vw_` views and `mv_` metric views
7. Cross-schema validation

Each phase reads the shared CORE Delta dimensions and must never recreate or
copy them.

One notebook also fixes the debugging experience: the previous layout ran each
domain as a child notebook via `dbutils.notebook.run`, which wraps every
failure in a generic `WorkflowException` and hides the real traceback in a
separate run. Here a failing cell stops Run All with the full traceback
inline.

## Implementation Scope

The multi-schema model below supersedes the former single-schema dataset.
`generate_banking_data.py` is the one-click entry point and generates every
table itself, phase by phase.

Repeated CORE names in this map mean that multiple Genie spaces reference the
same physical dimension. They do not represent copied `parties`, `products`,
`branches`, `employees`, or calendar tables.

Domain-specific Genie benchmarks are intentionally outside this implementation.
The existing benchmark loader remains unchanged until each Genie space is
configured and benchmark coverage is requested separately.

## Target Shared Core

Every domain uses the same conformed identifiers. Generate one physical copy of
these tables in CORE; fact generators and Genie spaces reference that copy with
fully qualified names.

| Table | Purpose | Used by |
|---|---|---|
| `parties` | People, businesses, and households with a `party_type` and shared relationship attributes | All domains |
| `party_relationships` | Household membership, beneficial ownership, authorized signer, and business relationships | Deposits, Commercial, Wealth, Financial Crime |
| `products` | Product catalog with business line, family, pricing, and lifecycle attributes | All product domains |
| `branches` | Branch and service-location dimension | Deposits, Service Operations, Commercial |
| `employees` | Branch staff, advisors, underwriters, collectors, investigators, and relationship managers | Lending, Commercial, Wealth, Service Operations, Financial Crime |
| `bank_calendar` | Consistent dates, fiscal periods, holidays, and business-day flags | All domains |

Retain `CORE.customers` as a compatibility view over person and organization
rows in `CORE.parties` while existing demos migrate to `party_id`.

```text
CORE shared dimensions (one physical copy)
  parties · party_relationships · products · branches · employees · bank_calendar
       │
       ├── Retail deposit/payment facts
       ├── Credit-card facts
       ├── Consumer-lending facts
       ├── Commercial-banking facts
       ├── Wealth-management facts
       ├── Service/operations facts
       ├── Financial-crime facts
       └── Finance/treasury facts
```

## Target Genie Spaces and Table Groups

The curated pre-joined view (`vw_`) and metric view (`mv_`) should be the
preferred starting objects in each space. Domain-owned source tables provide
record-level drill-through, while the last column lists shared dimensions
referenced from the single core copy.

| Genie space | Domain schema and owned objects | Shared CORE dimensions referenced |
|---|---|---|
| Bigly Bank - Retail Deposits & Payments | RETAIL: `vw_retail_deposits`, `mv_retail_deposits`, `deposit_accounts`, `deposit_transactions`, `deposit_balance_snapshots`, `payment_events`, `fee_events` | `CORE.parties`, `CORE.products`, `CORE.branches`, `CORE.bank_calendar` |
| Bigly Bank - Credit Cards | RETAIL: `vw_credit_cards`, `mv_credit_cards`, `card_accounts`, `cards`, `card_transactions`, `card_statements`, `card_payments`, `card_disputes`, `card_reward_events` | `CORE.parties`, `CORE.products`, `CORE.bank_calendar` |
| Bigly Bank - Consumer Lending | RETAIL: `vw_consumer_lending`, `mv_consumer_lending`, `loan_applications`, `credit_decisions`, `consumer_loans`, `loan_collateral`, `loan_payment_schedule`, `loan_payments`, `delinquency_snapshots`, `collection_actions` | `CORE.parties`, `CORE.products`, `CORE.employees`, `CORE.bank_calendar` |
| Bigly Bank - Small Business & Commercial | COMMERCIAL: `vw_commercial_banking`, `mv_commercial_banking`, `business_profiles`, `commercial_deposit_accounts`, `commercial_transactions`, `credit_facilities`, `commercial_loans`, `covenant_snapshots`, `merchant_settlements` | `CORE.parties`, `CORE.party_relationships`, `CORE.products`, `CORE.branches`, `CORE.employees`, `CORE.bank_calendar` |
| Bigly Bank - Wealth Management | WEALTH: `vw_wealth_management`, `mv_wealth_management`, `wealth_accounts`, `portfolios`, `securities`, `holding_snapshots`, `trades`, `advisory_fees`, `client_goals` | `CORE.parties`, `CORE.party_relationships`, `CORE.products`, `CORE.employees`, `CORE.bank_calendar` |
| Bigly Bank - Service & Branch Operations | OPERATIONS: `vw_service_operations`, `mv_service_operations`, `service_requests`, `complaints`, `customer_interactions`, `operational_incidents`, `branch_staffing_snapshots`, `branch_monthly_performance` | `CORE.parties`, `CORE.products`, `CORE.branches`, `CORE.employees`, `CORE.bank_calendar` |
| Bigly Bank - Fraud, AML & KYC | RISK: `vw_financial_crime`, `mv_financial_crime`, `transaction_alerts`, `fraud_cases`, `aml_cases`, `kyc_reviews`, `case_actions`, `fraud_loss_events` | `CORE.parties`, `CORE.party_relationships`, `CORE.products`, `CORE.employees`, `CORE.bank_calendar` |
| Bigly Bank - Finance & Treasury | FINANCE: `vw_bank_finance`, `mv_bank_finance`, `general_ledger_monthly`, `funds_transfer_pricing`, `product_profitability_monthly`, `credit_loss_provisions`, `liquidity_snapshots` | `CORE.products`, `CORE.branches`, `CORE.bank_calendar` |

Cross-schema fact dependencies are intentionally limited:

| Consumer | Upstream facts read | Purpose |
|---|---|---|
| OPERATIONS generator | `RETAIL.deposit_transactions` | Reconcile branch activity and fee revenue with originating retail ledger events |
| RISK generator and `RISK.vw_financial_crime` | `RETAIL.deposit_transactions`, `RETAIL.card_transactions`, `COMMERCIAL.commercial_transactions` | Create and explain alerts from originating transactions |
| FINANCE generator | RETAIL balance and loan snapshots, COMMERCIAL exposure and ledger activity, WEALTH fee and AUM snapshots, OPERATIONS cost snapshots, RISK fraud losses | Produce reconciled bank-level monthly financial aggregates |

The downstream schema stores its own alerts, cases, or aggregates. It does not
copy complete upstream transaction or dimension tables. OPERATIONS aggregates
retail activity into branch-month performance without copying the ledger.

Finance & Treasury is optional for the first implementation. Add it when the
demo needs CFO, asset-liability management, or enterprise profitability
questions rather than customer and product operations.

## Initial Scale and Grain

Targets are deterministic for a fixed seed. Small variations are acceptable
for child tables derived from weighted ownership and lifecycle rules.

| Schema | Primary objects | Grain | Initial scale |
|---|---|---|---:|
| CORE | `parties` | One person, business, or household | 25,000 |
| CORE | `party_relationships` | One relationship between two parties | ~15,000 |
| RETAIL | `deposit_accounts` | One deposit account | ~30,000 |
| RETAIL | `deposit_transactions` | One posted or reversed ledger event | ~750,000 |
| RETAIL | `deposit_balance_snapshots` | One account-month | ~900,000 |
| RETAIL | `card_accounts`, `cards` | One account or issued card | ~12,000 accounts; ~14,000 cards |
| RETAIL | `card_transactions` | One authorization or posted card event | ~400,000 |
| RETAIL | `card_statements`, `card_payments` | One statement cycle or payment | ~550,000 combined |
| RETAIL | `loan_applications`, `consumer_loans` | One application or booked loan | ~15,000 applications; ~9,000 loans |
| RETAIL | `loan_payment_schedule`, `loan_payments`, `delinquency_snapshots` | One scheduled payment, actual payment, or loan-month | ~750,000 combined |
| COMMERCIAL | Deposits, transactions, facilities, loans, and settlements | One business relationship event or snapshot | ~275,000 combined facts |
| WEALTH | `holding_snapshots`, `trades` | One portfolio-security-month or trade | ~300,000 combined |
| OPERATIONS | `customer_interactions`, `service_requests` | One interaction or service case | ~150,000 combined |
| RISK | Alerts, cases, reviews, actions, and loss events | One risk workflow event | ~50,000 combined |
| FINANCE | Monthly profitability, provision, and liquidity records | One product, branch, or business-line month | 5,000–20,000 |

## Notebook Interface Contract

| Widget | Requirement |
|---|---|
| `catalog` | Required Unity Catalog; fail fast when empty |
| `schema_prefix` | Required user-supplied schema prefix; `bigly_bank` is recommended for this demo |
| `seed` | Shared deterministic seed; initial value `42` |
| `as_of_date` | Shared inclusive end date for lifecycle generation |
| `notebook_base_path` | Retained for job compatibility; unused (no child notebooks) |
| `enable_finance` | Optional boolean controlling the FINANCE phase |

The notebook exposes each of these as an editable `DEFAULT_*` constant in its
Configuration cell; widget values and job parameters override the constants.
Each phase records a summary dict in the shared `results` object, which the
final cell prints and returns as the notebook exit value. Failures surface as
normal cell tracebacks — there are no child notebooks and no wrapper
exceptions.

## How to Run

Prerequisites are a Unity Catalog-enabled workspace, permission to create
schemas and managed tables in the chosen catalog, and Databricks Runtime 17.2+
or a serverless notebook (environment version 5+) for metric-view YAML
version 1.1.

Open `generate_banking_data.py`, set `DEFAULT_CATALOG` and
`DEFAULT_SCHEMA_PREFIX` in its Configuration cell (or supply the `catalog` and
`schema_prefix` widget values), and click Run All. Set `enable_finance` to
`true` only when the optional Finance & Treasury schema is wanted. Phases run
in dependency order, create the semantic objects, and finish with validation.

## Running as a Databricks Job

The notebook accepts job parameters through its widgets. Run it directly as a
single notebook task in a Databricks Job:

- Create a **Notebook** task pointing at `generate_banking_data.py`.
- Pass `catalog`, `schema_prefix`, `seed`, `as_of_date`, and
  `enable_finance` as base parameters (or set the `DEFAULT_*` constants in
  the Configuration cell); each maps to the notebook's widget values.
- Schedule it on classic job compute (DBR 17.2+) or serverless job compute
  (environment version 5). Serverless includes `faker` as an environment
  dependency; on classic compute the notebook's pinned `%pip` cell installs
  it.

A failing cell fails the task with the real traceback in the task logs — no
child-run lookup required.

## Space Boundaries

### Retail Deposits & Payments

Route here:

- "How are checking and savings balances trending by customer tier?"
- "Which channels are driving deposit growth?"
- "Where did overdraft fee revenue increase?"

Route card purchase and utilization questions to Credit Cards. Route loan
payments and delinquency to Consumer Lending.

### Credit Cards

Route here:

- "Which merchant categories drove the holiday spend increase?"
- "How is revolving utilization changing by risk band?"
- "What is our dispute and chargeback rate?"

Route fraud investigations to Financial Crime even when the originating event
is a card transaction.

### Consumer Lending

Route here:

- "How did mortgage approval and pull-through rates change?"
- "Which regions have rising 30-plus-day delinquency?"
- "How much principal is scheduled versus actually collected?"

Commercial facilities and business loans belong in Small Business & Commercial.

### Small Business & Commercial

Route here:

- "Which industries have the highest utilized credit exposure?"
- "Which businesses are approaching covenant limits?"
- "How do cash-management deposits affect relationship profitability?"

Personal deposit and consumer loan questions belong in their retail spaces.

### Wealth Management

Route here:

- "How is AUM changing by advisor and client segment?"
- "Which portfolios have drifted from their target allocation?"
- "What advisory fees and net flows did we generate?"

Retail deposit balances held outside managed portfolios belong in Retail
Deposits & Payments.

### Service & Branch Operations

Route here:

- "Which incident caused the complaint and call-volume spike?"
- "Where are resolution SLAs being missed?"
- "Which branches have high operating cost relative to activity?"

Product balances, purchases, and loan performance remain authoritative in the
corresponding product spaces.

### Fraud, AML & KYC

Route here:

- "Which alert typologies generate the most confirmed cases?"
- "What are fraud losses and recoveries by channel?"
- "Which KYC reviews are overdue for high-risk parties?"

This space owns investigation outcomes. Product spaces own ordinary transaction
and account performance.

### Finance & Treasury

Route here:

- "How did net interest income and margin change by product?"
- "Which business lines generated the most risk-adjusted profit?"
- "How are liquidity and deposit concentration trending?"

This space owns bank-level financial measures. Product spaces own customer and
account-level operating measures.

## Cross-Domain Story Links

Use common keys so domain data tells connected stories rather than independent
random patterns.

| Story | Required links | Expected spaces |
|---|---|---|
| A mobile outage causes failed payments, fee reversals, complaints, and branch traffic | `incident_id`, `party_id`, `account_id`, event timestamp | Deposits & Payments, Service & Branch Operations |
| Higher rates drive movement from checking to high-yield savings while reducing mortgage demand | `party_id`, `product_id`, calendar month | Deposits & Payments, Consumer Lending, Finance & Treasury |
| A regional downturn increases small-business utilization, consumer delinquency, and provisions | region, industry, calendar month, loan identifiers | Consumer Lending, Commercial, Finance & Treasury |
| Holiday card fraud creates alerts, disputes, losses, and recoveries | `transaction_id`, `alert_id`, `case_id`, `dispute_id` | Credit Cards, Fraud, AML & KYC |
| Wealth clients move cash into managed portfolios after an advisor campaign | `party_id`, `household_id`, campaign date | Deposits & Payments, Wealth Management |

## Generation and Data-Contract Rules

- Keep all implementation files as Databricks-exported source notebooks and
  preserve `# Databricks notebook source` and `# COMMAND ----------` markers.
- Require the user-supplied `catalog` and `schema_prefix`; do not silently fall
  back to a catalog or schema.
- Run `CREATE SCHEMA IF NOT EXISTS` for each target schema before writing its
  managed Delta tables.
- Use fully qualified three-part object names for every cross-schema read,
  constraint, curated view, and metric-view source.
- Generate shared parents first and enforce valid foreign keys in every domain.
- Generate CORE dimensions once. Domain notebooks read CORE and never copy its
  tables into their own schemas.
- Use one fixed seed and one `as_of_date` across all generators.
- Use Spark transformations, `spark.range`, joins, and window functions for
  high-volume generation; do not use driver-side row loops or `.collect()`.
- Never generate activity before account opening or after account closure.
- Keep deposit, card, and loan balance semantics separate. Store explicit signed
  amounts and bank-versus-customer accounting direction where appropriate.
- Generate chronological events before calculating balances and snapshots.
- Connect alerts, disputes, cases, complaints, and incidents to their originating
  events rather than assigning unrelated random flags.
- Give high-volume fact tables enough rows for monthly and cohort trends; avoid
  a few events per account over a multi-year period.
- Pin notebook-scoped library installs (for example `%pip install
  faker==40.36.0`). Unpinned installs are not compatible with serverless
  compute, and `%restart_python` must not run on serverless because it
  reinitializes the notebook environment.
- Create a curated `vw_` SQL view in the authoritative domain schema only when
  it simplifies joins; do not materialize duplicate dimension data in it.
- Create one `mv_` metric view per Genie space with documented grain, measures,
  dimensions, synonyms, PK/FK relationships, and comments.
- Make full runs reproducible and idempotent with overwrite behavior. Incremental
  append mode is outside the initial implementation.
- Use synthetic names only; do not generate emails, account numbers resembling
  production identifiers, secrets, or real customer data.
- Validate lifecycle dates, orphan keys, balance reconciliation, expected
  distributions, and injected story outcomes after all domains finish.

## Recommended Delivery Phases

1. Correct and separate the current Deposits, Cards, Lending, and Service data.
2. Add true Small Business & Commercial and Financial Crime domains.
3. Add Wealth Management.
4. Add Finance & Treasury if enterprise financial analysis is in scope.

This sequence delivers useful independent Genie spaces early while preserving
shared identifiers for later cross-space routing.
