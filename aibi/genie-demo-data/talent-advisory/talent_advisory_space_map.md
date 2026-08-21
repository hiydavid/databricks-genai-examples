# Talent Advisory Multi-Genie Space Map

This guide maps the `talent_advisory` synthetic dataset into focused Genie spaces
for Genie One Chat style demos. It does not create spaces programmatically; use it
as the curation plan when configuring each Genie space.

## Recommended Spaces

| Genie space | Primary authority | Recommended objects |
|---|---|---|
| Talent Advisor - Workforce Planning | Headcount, growth, exits, vacancy, and org capacity | `mart_workforce_planning`, `mv_workforce_planning`, `org_units`, `job_roles`, `locations`, `requisitions` |
| Talent Advisor - Hiring Funnel | Requisitions, applications, interviews, offers, acceptance, and time-to-fill | `mart_hiring_funnel`, `mv_hiring_funnel`, `requisitions`, `applications`, `job_roles`, `org_units` |
| Talent Advisor - Retention & Engagement | Engagement, burnout, flight risk, regretted attrition, and retention actions | `mart_retention_engagement`, `mv_retention_engagement`, `engagement_pulses`, `retention_risk_snapshots`, `talent_events`, `employees` |
| Talent Advisor - Mobility & Skills | Promotions, transfers, learning, skills gaps, internal fill, and mobility program impact | `mart_internal_mobility`, `mv_internal_mobility`, `employee_skills`, `learning_activity`, `skills`, `talent_events` |
| Talent Advisor - Compensation & Performance | Salary, bonus, compa-ratio, range penetration, performance, and high-risk compensation segments | `mart_comp_performance`, `mv_comp_performance`, `compensation_snapshots`, `performance_reviews`, `job_roles`, `employees` |
| Talent Advisor - Succession & Critical Roles | Critical roles, successor coverage, readiness, weak coverage, and bench strength | `mart_succession_planning`, `mv_succession_planning`, `succession_plans`, `org_units`, `job_roles`, `employees` |

Keep each space to the recommended objects above unless a demo requires deeper
drill-down. The mart and metric view should be the preferred starting point in
each space; source tables are included for explainability and drill-through.

## Space Boundaries

### Talent Advisor - Workforce Planning

Use this space for questions about headcount, hiring, exits, org growth, vacancy,
and staffing capacity.

Route here:
- "Which orgs grew fastest in 2025?"
- "Where do we have the highest vacancy rate?"
- "Show monthly headcount trend by business unit."

Route elsewhere:
- Candidate funnel conversion belongs in Hiring Funnel.
- Flight risk drivers belong in Retention & Engagement.
- Succession coverage belongs in Succession & Critical Roles.

### Talent Advisor - Hiring Funnel

Use this space for recruiting funnel, time-to-fill, offer acceptance, internal
fill, and candidate market questions.

Route here:
- "Which job families have the longest time to fill?"
- "Are Product offers below market midpoint being declined more often?"
- "How did internal fill rate change in 2025?"

Route elsewhere:
- Internal moves after hire belong in Mobility & Skills.
- Headcount capacity belongs in Workforce Planning.
- Compensation outcomes for employees belong in Compensation & Performance.

### Talent Advisor - Retention & Engagement

Use this space for engagement pulse trends, burnout, retention risk, regretted
exits, risk drivers, and recommended retention actions.

Route here:
- "Which orgs have the highest high-risk employee count?"
- "What happened to Sales East engagement in 2024?"
- "Do mobility program participants have lower retention risk?"

Route elsewhere:
- Salary band analysis belongs in Compensation & Performance.
- Skill-gap remediation belongs in Mobility & Skills.
- Open requisitions belong in Workforce Planning or Hiring Funnel.

### Talent Advisor - Mobility & Skills

Use this space for promotions, transfers, mobility program impact, learning
activity, skills gaps, and internal fill support.

Route here:
- "Which functions have the most critical skill gaps?"
- "Did the 2025 mobility program increase promotions or transfers?"
- "Where are Data & AI skill gaps highest?"

Route elsewhere:
- Offer acceptance belongs in Hiring Funnel.
- Engagement risk belongs in Retention & Engagement.
- Succession readiness belongs in Succession & Critical Roles.

### Talent Advisor - Compensation & Performance

Use this space for compa-ratio, salary, bonus, range penetration, performance,
high performers below market, and comp-related retention segments.

Route here:
- "Where do we have high performers below 0.90 compa-ratio?"
- "What is average compa-ratio by job family and level?"
- "Are low compa-ratio high performers showing higher risk?"

Route elsewhere:
- Hiring offers belong in Hiring Funnel.
- Succession coverage belongs in Succession & Critical Roles.
- Engagement pulse details belong in Retention & Engagement.

### Talent Advisor - Succession & Critical Roles

Use this space for critical role coverage, successor readiness, weak coverage,
and bench strength.

Route here:
- "Which critical roles have no ready successor?"
- "Where is GTM succession coverage weakest?"
- "Compare Data & AI Platform succession coverage to other orgs."

Route elsewhere:
- Hiring pipeline for those roles belongs in Hiring Funnel.
- Skill development plans belong in Mobility & Skills.
- Headcount plans belong in Workforce Planning.

## Cross-Space Demo Prompts

Use these prompts to show Genie One Chat routing across multiple spaces.

| Prompt | Expected spaces |
|---|---|
| "Which orgs have the highest attrition risk and the weakest hiring pipeline?" | Retention & Engagement, Hiring Funnel |
| "Where are we growing fastest but lacking internal successors?" | Workforce Planning, Succession & Critical Roles |
| "Are under-market high performers more likely to leave?" | Compensation & Performance, Retention & Engagement |
| "Which Data & AI teams have high skill gaps and long time-to-fill?" | Mobility & Skills, Hiring Funnel |
| "Did the 2025 mobility program improve internal fill and reduce flight risk?" | Mobility & Skills, Hiring Funnel, Retention & Engagement |
| "Which critical GTM roles need hiring support and stronger succession plans?" | Succession & Critical Roles, Hiring Funnel, Workforce Planning |

## Suggested Starting Instructions

Use a short topic-specific instruction block in each Genie space:

```text
You are a talent advisor for a fictional mid-market technology company.
Use the curated mart and metric view first for this space's topic. Use source
tables only when the user asks for record-level detail or root-cause drill-down.
Do not infer protected-class attributes; this synthetic dataset intentionally
omits them.
```

Add one extra sentence per space that states its authority boundary. For example:

```text
This space is authoritative for retention risk, engagement, burnout, regretted
attrition, and retention action questions.
```

## Notes

- The dataset is synthetic and reproducible with seed `42`.
- Employee names are fake; emails, phone numbers, home addresses, and protected
  class attributes are intentionally absent.
- Exact compensation values are synthetic and exist to support compensation and
  performance demos.
- The mart tables intentionally overlap on dimensions such as org, job family,
  role, and year so cross-space answers can be compared cleanly.
