# Databricks notebook source

# MAGIC %md
# MAGIC # Bigly Bank — Service and Branch Operations
# MAGIC
# MAGIC Generates incidents, interactions, service requests, complaints,
# MAGIC staffing snapshots, and branch monthly performance in OPERATIONS.

# COMMAND ----------

from datetime import date
import json

from pyspark.sql import functions as F

try:
    dbutils.widgets.text("catalog", "", "Unity Catalog (required)")
except Exception:
    pass
try:
    dbutils.widgets.text("schema_prefix", "", "Schema prefix (required)")
except Exception:
    pass
try:
    dbutils.widgets.text("seed", "42", "Deterministic seed")
except Exception:
    pass
try:
    dbutils.widgets.text("as_of_date", "2025-12-31", "Inclusive as-of date")
except Exception:
    pass

CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA_PREFIX = dbutils.widgets.get("schema_prefix").strip()
SEED = int(dbutils.widgets.get("seed"))
AS_OF_DATE = date.fromisoformat(dbutils.widgets.get("as_of_date").strip())

if not CATALOG:
    raise ValueError("catalog is required")
if not SCHEMA_PREFIX:
    raise ValueError("schema_prefix is required")
if "`" in CATALOG or "`" in SCHEMA_PREFIX:
    raise ValueError("catalog and schema_prefix cannot contain backticks")

CORE_SCHEMA = f"{SCHEMA_PREFIX}_core"
RETAIL_SCHEMA = f"{SCHEMA_PREFIX}_retail"
OPERATIONS_SCHEMA = f"{SCHEMA_PREFIX}_operations"
CORE = f"`{CATALOG}`.`{CORE_SCHEMA}`"
RETAIL = f"`{CATALOG}`.`{RETAIL_SCHEMA}`"
OPERATIONS = f"`{CATALOG}`.`{OPERATIONS_SCHEMA}`"
START_DATE = date(AS_OF_DATE.year - 2, 1, 1)

INCIDENT_COUNT = 25
SERVICE_REQUEST_COUNT = 50_000
INTERACTION_COUNT = 100_000
PERSON_COUNT = 20_000
BRANCH_COUNT = 40
EMPLOYEE_COUNT = 800

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {OPERATIONS}")


def stable_fraction(*columns):
    return F.pmod(F.xxhash64(*columns, F.lit(SEED)), F.lit(1_000_000)) / F.lit(
        1_000_000.0
    )


def choose(values, selector):
    return F.element_at(F.array(*[F.lit(v) for v in values]), selector + F.lit(1))


def write_table(df, table_name, comment):
    full_name = f"{OPERATIONS}.`{table_name}`"
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(full_name)
    )
    spark.sql(f"COMMENT ON TABLE {full_name} IS '{comment.replace(chr(39), chr(39) * 2)}'")
    print(f"Wrote {full_name}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Operational incidents

# COMMAND ----------

systems = ["Mobile Banking", "Online Banking", "Payments", "Card Authorization", "CRM", "Branch Network"]
incident_types = ["Service Outage", "Performance Degradation", "Release Defect", "Network Failure"]

operational_incidents = (
    spark.range(INCIDENT_COUNT, numPartitions=2)
    .withColumn("incident_number", F.col("id") + 1)
    .withColumn(
        "incident_id",
        F.when(F.col("id") == 0, "INC-MOBILE-20240115").otherwise(F.concat(F.lit("INC-"), F.lpad(F.col("incident_number").cast("string"), 6, "0"))),
    )
    .withColumn(
        "started_at",
        F.when(F.col("id") == 0, F.to_timestamp(F.lit("2024-01-15 08:15:00"))).otherwise(
            F.to_timestamp(
                F.concat_ws(
                    " ",
                    F.date_add(F.lit(START_DATE), F.pmod(F.xxhash64("id", F.lit("incident-date")), F.lit((AS_OF_DATE - START_DATE).days + 1)).cast("int")),
                    F.lit("09:00:00"),
                )
            )
        ),
    )
    .withColumn("duration_minutes", F.when(F.col("id") == 0, 1_080).otherwise(F.lit(30) + F.pmod(F.xxhash64("id", F.lit("duration")), F.lit(540))))
    .withColumn("ended_at", F.expr("timestampadd(MINUTE, duration_minutes, started_at)"))
    .withColumn("affected_system", F.when(F.col("id") == 0, "Mobile Banking").otherwise(choose(systems, F.pmod(F.xxhash64("id", F.lit("system")), F.lit(len(systems))))))
    .withColumn("incident_type", F.when(F.col("id") == 0, "Service Outage").otherwise(choose(incident_types, F.pmod(F.xxhash64("id", F.lit("incident-type")), F.lit(len(incident_types))))))
    .withColumn("severity", F.when(F.col("id") == 0, "Critical").when(stable_fraction("id", F.lit("severity")) < 0.25, "High").when(stable_fraction("id", F.lit("severity")) < 0.70, "Medium").otherwise("Low"))
    .withColumn("root_cause", F.when(F.col("id") == 0, "Authentication service release defect").otherwise("Synthetic infrastructure or application failure"))
    .withColumn("estimated_impact_usd", F.round(F.when(F.col("id") == 0, 2_350_000.0).otherwise(F.lit(10_000.0) + stable_fraction("id", F.lit("impact")) * 350_000.0), 2))
    .withColumn("status", F.lit("Resolved"))
    .select("incident_id", "started_at", "ended_at", "duration_minutes", "affected_system", "incident_type", "severity", "root_cause", "estimated_impact_usd", "status")
)

write_table(operational_incidents, "operational_incidents", "Operational incidents, including the January 2024 mobile-banking outage narrative.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customer interactions, cases, and complaints

# COMMAND ----------

party_lookup = spark.table(f"{CORE}.`parties`").select("party_id", "relationship_tier", "region", "state")

service_requests = (
    spark.range(SERVICE_REQUEST_COUNT, numPartitions=16)
    .withColumn("request_number", F.col("id") + 1)
    .withColumn("u_date", stable_fraction("id", F.lit("request-date")))
    .withColumn("u_category", stable_fraction("id", F.lit("request-category")))
    .withColumn("is_outage_cohort", F.col("u_date") < 0.18)
    .withColumn(
        "request_date",
        F.when(
            F.col("is_outage_cohort"),
            F.date_add(F.lit(date(2024, 1, 15)), F.pmod(F.xxhash64("id", F.lit("outage-day")), F.lit(17)).cast("int")),
        ).otherwise(F.date_add(F.lit(START_DATE), F.pmod(F.xxhash64("id", F.lit("normal-day")), F.lit((AS_OF_DATE - START_DATE).days + 1)).cast("int"))),
    )
    .withColumn(
        "party_id",
        F.concat(F.lit("PTY-"), F.lpad((F.pmod(F.xxhash64("id", F.lit("request-party")), F.lit(PERSON_COUNT)) + 1).cast("string"), 6, "0")),
    )
    .join(party_lookup, "party_id")
    .withColumn("request_id", F.concat(F.lit("SRQ-"), F.lpad(F.col("request_number").cast("string"), 8, "0")))
    .withColumn(
        "category",
        F.when(F.col("is_outage_cohort") & (F.col("u_category") < 0.60), "Complaint")
        .when(F.col("is_outage_cohort"), "Technical Issue")
        .when(F.col("u_category") < 0.25, "Account Inquiry")
        .when(F.col("u_category") < 0.43, "Payment Issue")
        .when(F.col("u_category") < 0.58, "Dispute")
        .when(F.col("u_category") < 0.73, "Complaint")
        .when(F.col("u_category") < 0.88, "Product Inquiry")
        .otherwise("Technical Issue"),
    )
    .withColumn(
        "channel",
        F.when(F.col("is_outage_cohort"), F.when(stable_fraction("id", F.lit("outage-channel")) < 0.58, "Phone").otherwise("Chat"))
        .when(stable_fraction("id", F.lit("channel")) < 0.36, "Phone")
        .when(stable_fraction("id", F.lit("channel")) < 0.65, "Chat")
        .when(stable_fraction("id", F.lit("channel")) < 0.84, "App")
        .otherwise("Branch"),
    )
    .withColumn("incident_id", F.when(F.col("is_outage_cohort"), "INC-MOBILE-20240115").cast("string"))
    .withColumn("branch_id", F.concat(F.lit("BRN-"), F.lpad((F.pmod(F.xxhash64("id", F.lit("service-branch")), F.lit(BRANCH_COUNT)) + 1).cast("string"), 4, "0")))
    .withColumn("assigned_employee_id", F.concat(F.lit("EMP-"), F.lpad((F.pmod(F.xxhash64("id", F.lit("service-agent")), F.lit(EMPLOYEE_COUNT)) + 1).cast("string"), 5, "0")))
    .withColumn("priority", F.when(F.col("category") == "Complaint", "High").when(F.col("category").isin("Payment Issue", "Dispute"), "Medium").otherwise("Low"))
    .withColumn("status", F.when(stable_fraction("id", F.lit("request-status")) < 0.80, "Resolved").when(stable_fraction("id", F.lit("request-status")) < 0.93, "Open").otherwise("Escalated"))
    .withColumn(
        "resolution_time_hours",
        F.when(
            F.col("status") == "Resolved",
            F.round(
                F.when(F.col("is_outage_cohort"), F.lit(18.0) + stable_fraction("id", F.lit("outage-resolution")) * 72.0)
                .when(F.col("priority") == "High", F.lit(8.0) + stable_fraction("id", F.lit("high-resolution")) * 48.0)
                .otherwise(F.lit(1.0) + stable_fraction("id", F.lit("normal-resolution")) * 20.0),
                1,
            ),
        ).cast("double"),
    )
    .withColumn(
        "satisfaction_score",
        F.when(
            F.col("status") == "Resolved",
            F.greatest(
                F.lit(1),
                F.least(
                    F.lit(5),
                    F.floor(F.lit(5.0) - F.coalesce("resolution_time_hours", F.lit(0.0)) / 24.0 - F.when(F.col("category") == "Complaint", 1.0).otherwise(0.0)).cast("int"),
                ),
            ),
        ).cast("int"),
    )
    .select("request_id", "request_date", "party_id", "branch_id", "assigned_employee_id", "channel", "category", "priority", "status", "resolution_time_hours", "satisfaction_score", "incident_id", "relationship_tier", "region", "state")
)

write_table(service_requests, "service_requests", "Customer service cases with outage-linked complaints, SLA performance, and satisfaction.")

complaints = (
    spark.table(f"{OPERATIONS}.`service_requests`")
    .filter(F.col("category") == "Complaint")
    .withColumn("complaint_id", F.concat(F.lit("CMP-"), F.substring("request_id", 5, 8)))
    .withColumn("complaint_reason", F.when(F.col("incident_id").isNotNull(), "Digital Access Failure").when(F.col("channel") == "Branch", "Branch Experience").otherwise("Product or Fee Concern"))
    .withColumn("regulatory_category", F.when(F.col("complaint_reason") == "Digital Access Failure", "Managing the Account").otherwise("Fees or Service"))
    .withColumn("remediation_amount_usd", F.when(F.col("status") == "Resolved", F.round(stable_fraction("request_id", F.lit("remediation")) * 150.0, 2)).otherwise(0.0))
    .select("complaint_id", "request_id", "request_date", "party_id", "branch_id", "complaint_reason", "regulatory_category", "status", "resolution_time_hours", "satisfaction_score", "remediation_amount_usd", "incident_id", "region")
)

write_table(complaints, "complaints", "Formal complaints linked to service requests, incidents, and remediation amounts.")

interaction_topics = ["Account Help", "Payment Help", "Card Help", "Loan Help", "Digital Help", "Product Advice"]
customer_interactions = (
    spark.range(INTERACTION_COUNT, numPartitions=16)
    .withColumn("interaction_number", F.col("id") + 1)
    .withColumn("is_outage_cohort", stable_fraction("id", F.lit("interaction-outage")) < 0.12)
    .withColumn(
        "interaction_date",
        F.when(F.col("is_outage_cohort"), F.date_add(F.lit(date(2024, 1, 15)), F.pmod(F.xxhash64("id", F.lit("interaction-outage-day")), F.lit(17)).cast("int")))
        .otherwise(F.date_add(F.lit(START_DATE), F.pmod(F.xxhash64("id", F.lit("interaction-day")), F.lit((AS_OF_DATE - START_DATE).days + 1)).cast("int"))),
    )
    .withColumn("party_id", F.concat(F.lit("PTY-"), F.lpad((F.pmod(F.xxhash64("id", F.lit("interaction-party")), F.lit(PERSON_COUNT)) + 1).cast("string"), 6, "0")))
    .join(party_lookup, "party_id")
    .withColumn("interaction_id", F.concat(F.lit("INT-"), F.lpad(F.col("interaction_number").cast("string"), 9, "0")))
    .withColumn("channel", F.when(F.col("is_outage_cohort"), "Phone").when(stable_fraction("id", F.lit("interaction-channel")) < 0.38, "Phone").when(stable_fraction("id", F.lit("interaction-channel")) < 0.65, "Chat").when(stable_fraction("id", F.lit("interaction-channel")) < 0.85, "Branch").otherwise("Secure Message"))
    .withColumn("topic", F.when(F.col("is_outage_cohort"), "Digital Help").otherwise(choose(interaction_topics, F.pmod(F.xxhash64("id", F.lit("interaction-topic")), F.lit(len(interaction_topics))))))
    .withColumn("branch_id", F.concat(F.lit("BRN-"), F.lpad((F.pmod(F.xxhash64("id", F.lit("interaction-branch")), F.lit(BRANCH_COUNT)) + 1).cast("string"), 4, "0")))
    .withColumn("employee_id", F.concat(F.lit("EMP-"), F.lpad((F.pmod(F.xxhash64("id", F.lit("interaction-employee")), F.lit(EMPLOYEE_COUNT)) + 1).cast("string"), 5, "0")))
    .withColumn("handle_time_minutes", F.round(F.when(F.col("is_outage_cohort"), 18.0 + stable_fraction("id", F.lit("outage-handle")) * 35.0).otherwise(3.0 + stable_fraction("id", F.lit("normal-handle")) * 18.0), 1))
    .withColumn("outcome", F.when(F.col("is_outage_cohort"), "Follow-up Required").when(stable_fraction("id", F.lit("interaction-outcome")) < 0.78, "Resolved").otherwise("Follow-up Required"))
    .withColumn("incident_id", F.when(F.col("is_outage_cohort"), "INC-MOBILE-20240115").cast("string"))
    .select("interaction_id", "interaction_date", "party_id", "branch_id", "employee_id", "channel", "topic", "handle_time_minutes", "outcome", "incident_id", "relationship_tier", "region")
)

write_table(customer_interactions, "customer_interactions", "Omnichannel customer contacts with outage-driven call volume and handle-time degradation.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Branch staffing and performance

# COMMAND ----------

branches = spark.table(f"{CORE}.`branches`")
months = spark.table(f"{CORE}.`bank_calendar`").select("month_start").distinct()

branch_staffing_snapshots = (
    branches.crossJoin(months)
    .withColumn("snapshot_date", F.last_day("month_start"))
    .withColumn("budgeted_fte", F.lit(12) + F.pmod(F.xxhash64("branch_id", F.lit("budgeted-fte")), F.lit(24)))
    .withColumn("actual_fte", F.greatest(F.lit(5), F.col("budgeted_fte") - F.pmod(F.xxhash64("branch_id", "month_start", F.lit("vacancy")), F.lit(5))))
    .withColumn("overtime_hours", F.round(F.greatest(F.lit(0.0), (F.col("budgeted_fte") - F.col("actual_fte")) * (15.0 + stable_fraction("branch_id", "month_start", F.lit("overtime")) * 25.0)), 1))
    .withColumn("monthly_staff_cost_usd", F.round(F.col("actual_fte") * (6_500.0 + stable_fraction("branch_id", F.lit("staff-cost")) * 2_500.0) + F.col("overtime_hours") * 45.0, 2))
    .select("snapshot_date", "month_start", "branch_id", "budgeted_fte", "actual_fte", "overtime_hours", "monthly_staff_cost_usd")
)

write_table(branch_staffing_snapshots, "branch_staffing_snapshots", "Monthly branch staffing, vacancies, overtime, and labor cost.")

monthly_requests = (
    spark.table(f"{OPERATIONS}.`service_requests`")
    .withColumn("month_start", F.trunc("request_date", "month"))
    .groupBy("branch_id", "month_start")
    .agg(F.count("*").alias("service_request_count"), F.sum(F.when(F.col("category") == "Complaint", 1).otherwise(0)).alias("complaint_count"))
)
monthly_interactions = (
    spark.table(f"{OPERATIONS}.`customer_interactions`")
    .withColumn("month_start", F.trunc("interaction_date", "month"))
    .groupBy("branch_id", "month_start")
    .agg(F.count("*").alias("interaction_count"), F.sum("handle_time_minutes").alias("total_handle_minutes"))
)
monthly_transactions = (
    spark.table(f"{RETAIL}.`deposit_transactions`")
    .withColumn("month_start", F.trunc("transaction_date", "month"))
    .groupBy("branch_id", "month_start")
    .agg(F.count("*").alias("transaction_count"), F.sum("fee_usd").alias("fee_revenue_usd"))
)

branch_monthly_performance = (
    branches.crossJoin(months)
    .join(monthly_requests, ["branch_id", "month_start"], "left")
    .join(monthly_interactions, ["branch_id", "month_start"], "left")
    .join(monthly_transactions, ["branch_id", "month_start"], "left")
    .join(spark.table(f"{OPERATIONS}.`branch_staffing_snapshots`").select("branch_id", "month_start", "actual_fte", "monthly_staff_cost_usd"), ["branch_id", "month_start"], "left")
    .fillna({"service_request_count": 0, "complaint_count": 0, "interaction_count": 0, "total_handle_minutes": 0.0, "transaction_count": 0, "fee_revenue_usd": 0.0})
    .withColumn("branch_visit_count", F.round(F.col("interaction_count") * 0.55 + F.col("transaction_count") * 0.08).cast("long"))
    .withColumn("non_staff_operating_cost_usd", F.round(F.lit(25_000.0) + stable_fraction("branch_id", "month_start", F.lit("branch-cost")) * 65_000.0, 2))
    .withColumn("total_operating_cost_usd", F.round(F.col("monthly_staff_cost_usd") + F.col("non_staff_operating_cost_usd"), 2))
    .withColumn("snapshot_date", F.last_day("month_start"))
    .select("snapshot_date", "month_start", "branch_id", "branch_name", "branch_type", "region", "state", "actual_fte", "branch_visit_count", "transaction_count", "service_request_count", "complaint_count", "interaction_count", "total_handle_minutes", "fee_revenue_usd", "total_operating_cost_usd")
)

write_table(branch_monthly_performance, "branch_monthly_performance", "Monthly branch activity, customer demand, fee revenue, staffing, and operating cost.")

print(f"Service operations generation complete: {CATALOG}.{OPERATIONS_SCHEMA}")
dbutils.notebook.exit(
    json.dumps(
        {
            "schema": f"{CATALOG}.{OPERATIONS_SCHEMA}",
            "operational_incidents": INCIDENT_COUNT,
            "service_requests": SERVICE_REQUEST_COUNT,
            "customer_interactions": INTERACTION_COUNT,
        }
    )
)
