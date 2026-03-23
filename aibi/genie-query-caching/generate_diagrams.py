"""Generate architecture diagrams for the 3 Genie query caching scenarios.

Run: python generate_diagrams.py
Requires: pip install graphviz  (and graphviz CLI: brew install graphviz)
"""

import graphviz

# ── Shared colours ──────────────────────────────────────────────────────────
USER_BG = "#DAEEF8"
CACHE_LB_BG = "#FFF3E0"
CACHE_VS_BG = "#F3E5F5"
GENIE_BG = "#E8F5E9"
SQL_BG = "#FFF9C4"
HIT_BG = "#FCE4EC"

NODE = dict(shape="box", style="rounded,filled", fillcolor="white",
            fontname="Helvetica", fontsize="11")
DIAMOND = dict(shape="diamond", style="filled", fillcolor="white",
               fontname="Helvetica", fontsize="10", width="2.5", height="1.4")
ANNOT = dict(shape="plaintext", fontname="Helvetica", fontsize="9")
EDGE = dict(fontname="Helvetica", fontsize="9")


def _defaults(g: graphviz.Digraph):
    g.attr(rankdir="TB", splines="true", nodesep="0.5", ranksep="0.6",
           fontname="Helvetica", fontsize="12", compound="true", dpi="150")
    g.attr("node", **NODE)
    g.attr("edge", **EDGE)


# ════════════════════════════════════════════════════════════════════════════
# Scenario 1: Lakebase + pgvector Only
# ════════════════════════════════════════════════════════════════════════════
def diagram_1():
    g = graphviz.Digraph("scenario1", format="png")
    g.attr(label="Scenario 1: Lakebase + pgvector Only\n ",
           labelloc="t", fontsize="16", fontname="Helvetica-Bold")
    _defaults(g)

    # ── User Layer ──
    with g.subgraph(name="cluster_user") as c:
        c.attr(label="User Layer", style="filled,rounded", fillcolor=USER_BG,
               color="#90CAF9", fontname="Helvetica-Bold", fontsize="11")
        c.node("user", "User", fillcolor="#BBDEFB", style="rounded,filled,dashed")
        c.node("supervisor", "Supervisor Agent", fillcolor="#D1C4E9")
        c.edge("user", "supervisor")

    # ── Decision ──
    g.node("d1", "1. exact-match + pgvector\nsimilarity\nthreshold ≥ 0.92", **DIAMOND)
    g.node("hit_resp", "HIT → return cached\nresponse", shape="box",
           style="rounded,filled", fillcolor=HIT_BG, fontsize="10")

    # ── Cache Layer ──
    with g.subgraph(name="cluster_cache") as c:
        c.attr(label="Cache Layer (Lakebase + pgvector)", style="filled,rounded",
               fillcolor=CACHE_LB_BG, color="#FFB74D",
               fontname="Helvetica-Bold", fontsize="11")
        c.node("lakebase", "Lakebase", style="rounded,filled,bold", fillcolor="white")
        c.node("lb_schema",
               "question_normalized TEXT\nembedding VECTOR(768)\n"
               "cached_sql TEXT\ncached_response JSONB", **ANNOT)
        c.node("lb_props",
               "HNSW index · ACID writes\nImmediate read-after-write\n"
               "Scale-to-zero", **ANNOT)
        c.edge("lakebase", "lb_schema", style="invis")
        c.edge("lb_schema", "lb_props", style="invis")

    # ── Genie API ──
    with g.subgraph(name="cluster_genie") as c:
        c.attr(label="Genie API (via MCP Gateway)", style="filled,rounded",
               fillcolor=GENIE_BG, color="#81C784",
               fontname="Helvetica-Bold", fontsize="11")
        c.node("rate_limiter", "Rate Limiter + Queue\nper-user token bucket")
        c.node("mcp_gw", "MCP Gateway")
        c.node("genie", "Genie")
        c.node("retry", "Retry + Backoff\ncustomer-built",
               fillcolor="#C8E6C9", style="rounded,filled")
        c.node("other_tools", "Other MCP Tools\nRAG, doc retrieval")
        c.node("genie_api", "Genie MCP → API\n~1,760 Spaces")
        c.edge("rate_limiter", "mcp_gw")
        c.edge("mcp_gw", "genie", label="Genie")
        c.edge("mcp_gw", "other_tools", label="other tools")
        c.edge("genie", "retry")
        c.edge("genie", "genie_api")

    g.node("insert_note", "response → INSERT to\nLakebase\n(immediate write)", **ANNOT)

    # ── SQL Execution ──
    with g.subgraph(name="cluster_sql") as c:
        c.attr(label="SQL Execution", style="filled,rounded",
               fillcolor=SQL_BG, color="#FFF176",
               fontname="Helvetica-Bold", fontsize="11")
        c.node("hit_sql", "HIT (SQL cache) → re-\nexecute SQL", **ANNOT)
        c.node("stmt_api", "Statement Execution API")
        c.node("warehouse", "Serverless SQL Warehouse")
        c.node("user_data", "User Data", fillcolor="#FFF9C4")
        c.edge("hit_sql", "stmt_api")
        c.edge("stmt_api", "warehouse")
        c.edge("warehouse", "user_data")

    # ── Main vertical flow (high weight to keep straight) ──
    g.edge("supervisor", "d1", weight="10")
    g.edge("d1", "lakebase", weight="10")  # main path goes through cache
    g.edge("lb_props", "rate_limiter", label="2. MISS → execute",
           ltail="cluster_cache", weight="10")
    # HIT branches right (lower weight, no rank constraint)
    g.edge("d1", "hit_resp", label="HIT", constraint="false")

    # Genie response write-back
    g.edge("genie_api", "insert_note", weight="5")
    g.edge("insert_note", "lakebase", style="dashed", label="write back",
           constraint="false")

    # HIT → SQL execution
    g.edge("hit_resp", "hit_sql", style="dashed", label="re-execute SQL")

    g.render("scenario1-lakebase-pgvector", directory=".", cleanup=True)
    print("  ✓ scenario1-lakebase-pgvector.png")


# ════════════════════════════════════════════════════════════════════════════
# Scenario 2: Vector Search Index Only
# ════════════════════════════════════════════════════════════════════════════
def diagram_2():
    g = graphviz.Digraph("scenario2", format="png")
    g.attr(label="Scenario 2: Vector Search Index Only\n ",
           labelloc="t", fontsize="16", fontname="Helvetica-Bold")
    _defaults(g)

    # ── User Layer ──
    with g.subgraph(name="cluster_user") as c:
        c.attr(label="User Layer", style="filled,rounded", fillcolor=USER_BG,
               color="#90CAF9", fontname="Helvetica-Bold", fontsize="11")
        c.node("user", "User", fillcolor="#BBDEFB", style="rounded,filled,dashed")
        c.node("supervisor", "Supervisor Agent", fillcolor="#D1C4E9")
        c.edge("user", "supervisor")

    # ── Decision ──
    g.node("d1", "1. Hybrid search (semantic\n+ BM25) with confidence\ntiering", **DIAMOND)
    g.node("hit_resp", "HIT → return / confirm\ncached SQL", shape="box",
           style="rounded,filled", fillcolor=HIT_BG, fontsize="10")

    # ── Cache Layer ──
    with g.subgraph(name="cluster_cache") as c:
        c.attr(label="Cache Layer (Vector Search + Delta Table)",
               style="filled,rounded", fillcolor=CACHE_VS_BG, color="#CE93D8",
               fontname="Helvetica-Bold", fontsize="11")
        c.node("vs_endpoint", "VS Endpoint", style="rounded,filled,bold", fillcolor="white")
        c.node("vs_props",
               "Hybrid: semantic + BM25\nBuilt-in reranking\n"
               "Managed embeddings", **ANNOT)
        c.node("vs_tiers",
               "Confidence tiering:\n≥ 0.90 auto-execute\n"
               "0.75–0.90 confirm\n< 0.75 fall through", **ANNOT)
        c.edge("vs_endpoint", "vs_props", style="invis")
        c.edge("vs_props", "vs_tiers", style="invis")

    # ── Genie API ──
    with g.subgraph(name="cluster_genie") as c:
        c.attr(label="Genie API (via MCP Gateway)", style="filled,rounded",
               fillcolor=GENIE_BG, color="#81C784",
               fontname="Helvetica-Bold", fontsize="11")
        c.node("rate_limiter", "Rate Limiter + Queue\nper-user token bucket")
        c.node("mcp_gw", "MCP Gateway")
        c.node("genie", "Genie")
        c.node("retry", "Retry + Backoff\ncustomer-built",
               fillcolor="#C8E6C9", style="rounded,filled")
        c.node("other_tools", "Other MCP Tools\nRAG, doc retrieval")
        c.node("genie_api", "Genie MCP → API\n~1,760 Spaces")
        c.edge("rate_limiter", "mcp_gw")
        c.edge("mcp_gw", "genie", label="Genie")
        c.edge("mcp_gw", "other_tools", label="other tools")
        c.edge("genie", "retry")
        c.edge("genie", "genie_api")

    g.node("append_note", "response → APPEND to\nDelta\n(sync lag before\nsearchable)", **ANNOT)

    # ── Delta Table ──
    with g.subgraph(name="cluster_delta") as c:
        c.attr(label="", style="filled,rounded", fillcolor=CACHE_VS_BG, color="#CE93D8")
        c.node("delta_table",
               "Delta Table\ncache_store\n\nquestion_text STRING\n"
               "cached_sql STRING\ncached_response STRING",
               fillcolor="#E1BEE7", fontsize="9")
        c.node("governed", "Governed via Unity Catalog", **ANNOT)
        c.edge("delta_table", "governed", style="invis")

    g.node("delta_sync", "Delta Sync\nauto-embed\n(seconds-minutes lag)", **ANNOT)

    # ── SQL Execution ──
    with g.subgraph(name="cluster_sql") as c:
        c.attr(label="SQL Execution", style="filled,rounded",
               fillcolor=SQL_BG, color="#FFF176",
               fontname="Helvetica-Bold", fontsize="11")
        c.node("hit_sql", "HIT (high conf.) → re-\nexecute SQL", **ANNOT)
        c.node("stmt_api", "Statement Execution API")
        c.node("warehouse", "Serverless SQL Warehouse")
        c.node("user_data", "User Data", fillcolor="#FFF9C4")
        c.edge("hit_sql", "stmt_api")
        c.edge("stmt_api", "warehouse")
        c.edge("warehouse", "user_data")

    # ── Main vertical flow ──
    g.edge("supervisor", "d1", weight="10")
    g.edge("d1", "vs_endpoint", weight="10")  # main path through cache
    g.edge("vs_tiers", "rate_limiter", label="2. MISS → execute",
           ltail="cluster_cache", weight="10")
    g.edge("d1", "hit_resp", label="HIT", constraint="false")

    # HIT → SQL
    g.edge("hit_resp", "hit_sql", style="dashed", label="re-execute SQL")

    # Genie response → Delta → VS
    g.edge("genie_api", "append_note", weight="5")
    g.edge("append_note", "delta_table", label="write")
    g.edge("delta_table", "delta_sync", style="dashed", dir="back", label="sync")
    g.edge("delta_sync", "vs_endpoint", style="dashed", label="embed",
           constraint="false")

    g.render("scenario2-vector-search", directory=".", cleanup=True)
    print("  ✓ scenario2-vector-search.png")


# ════════════════════════════════════════════════════════════════════════════
# Scenario 3: Hybrid — Lakebase L1 + Vector Search L2 (Recommended)
# ════════════════════════════════════════════════════════════════════════════
def diagram_3():
    g = graphviz.Digraph("scenario3", format="png")
    g.attr(label="Scenario 3: Hybrid — Lakebase L1 + Vector Search L2 (Recommended)\n ",
           labelloc="t", fontsize="16", fontname="Helvetica-Bold")
    _defaults(g)

    # ── User Layer ──
    with g.subgraph(name="cluster_user") as c:
        c.attr(label="User Layer", style="filled,rounded", fillcolor=USER_BG,
               color="#90CAF9", fontname="Helvetica-Bold", fontsize="11")
        c.node("user", "User", fillcolor="#BBDEFB", style="rounded,filled,dashed")
        c.node("supervisor", "Supervisor Agent", fillcolor="#D1C4E9")
        c.edge("user", "supervisor")

    # ── L1 Decision ──
    g.node("d1", "1. exact-match + pgvector\nsimilarity\nthreshold ≥ 0.93", **DIAMOND)
    g.node("hit_resp", "HIT → return cached\nresponse", shape="box",
           style="rounded,filled", fillcolor=HIT_BG, fontsize="10")

    # ── L1: Session Cache ──
    with g.subgraph(name="cluster_l1") as c:
        c.attr(label="L1: Session Cache (Lakebase + pgvector)",
               style="filled,rounded", fillcolor=CACHE_LB_BG, color="#FFB74D",
               fontname="Helvetica-Bold", fontsize="11")
        c.node("lakebase", "Lakebase", style="rounded,filled,bold", fillcolor="white")
        c.node("l1_schema",
               "question_normalized TEXT\nembedding VECTOR(768)\n"
               "cached_response JSONB", **ANNOT)
        c.node("l1_props",
               "HNSW index · ACID writes\nTTL: session lifetime\n"
               "Scale-to-zero", **ANNOT)
        c.edge("lakebase", "l1_schema", style="invis")
        c.edge("l1_schema", "l1_props", style="invis")

    # ── L2 flow annotations ──
    g.node("l2_miss", "2. L1 MISS → hybrid search\nthreshold ≥ 0.90", **ANNOT)
    g.node("full_miss_note", "3. Full MISS → execute", **ANNOT)

    # ── L2: Knowledge Base ──
    with g.subgraph(name="cluster_l2") as c:
        c.attr(label="L2: Knowledge Base (Vector Search)",
               style="filled,rounded", fillcolor=CACHE_VS_BG, color="#CE93D8",
               fontname="Helvetica-Bold", fontsize="11")
        c.node("vs_endpoint", "VS Endpoint", style="rounded,filled,bold", fillcolor="white")
        c.node("vs_props",
               "Hybrid: semantic + BM25\nBuilt-in reranking\n"
               "Managed embeddings\nTTL: days-weeks", **ANNOT)
        c.node("promote_note", "promote after validation\n(thumbs-up, repeated use)", **ANNOT)
        c.node("delta_sync", "Delta Sync\nauto-embed", **ANNOT)
        c.node("delta_table",
               "Delta Table\ncache_knowledge_base\nValidated entries only\n"
               "Governed via Unity Catalog",
               fillcolor="#E1BEE7", fontsize="9")
        c.edge("vs_endpoint", "vs_props", style="invis")
        c.edge("promote_note", "delta_sync", style="dashed")
        c.edge("delta_sync", "delta_table")

    # ── Genie API ──
    with g.subgraph(name="cluster_genie") as c:
        c.attr(label="Genie API (via MCP Gateway)", style="filled,rounded",
               fillcolor=GENIE_BG, color="#81C784",
               fontname="Helvetica-Bold", fontsize="11")
        c.node("rate_limiter", "Rate Limiter + Queue\nper-user token bucket")
        c.node("mcp_gw", "MCP Gateway")
        c.node("genie", "Genie")
        c.node("retry", "Retry + Backoff\ncustomer-built",
               fillcolor="#C8E6C9", style="rounded,filled")
        c.node("other_tools", "Other MCP Tools\nRAG, doc retrieval")
        c.node("genie_api", "Genie MCP → API\n~1,760 Spaces")
        c.edge("rate_limiter", "mcp_gw")
        c.edge("mcp_gw", "genie", label="Genie")
        c.edge("mcp_gw", "other_tools", label="other tools")
        c.edge("genie", "retry")
        c.edge("genie", "genie_api")

    g.node("write_l1", "response → write to L1", **ANNOT)

    # ── SQL Execution ──
    with g.subgraph(name="cluster_sql") as c:
        c.attr(label="SQL Execution", style="filled,rounded",
               fillcolor=SQL_BG, color="#FFF176",
               fontname="Helvetica-Bold", fontsize="11")
        c.node("hit_sql", "HIT (high conf.) → re-\nexecute SQL", **ANNOT)
        c.node("stmt_api", "Statement Execution API")
        c.node("warehouse", "Serverless SQL Warehouse")
        c.node("user_data", "User Data", fillcolor="#FFF9C4")
        c.edge("hit_sql", "stmt_api")
        c.edge("stmt_api", "warehouse")
        c.edge("warehouse", "user_data")

    # ── Main vertical flow ──
    g.edge("supervisor", "d1", weight="10")
    g.edge("d1", "lakebase", weight="10")  # main path through L1
    g.edge("d1", "hit_resp", label="HIT", constraint="false")

    # L1 MISS → L2
    g.edge("l1_props", "l2_miss", ltail="cluster_l1", weight="8")
    g.edge("l2_miss", "vs_endpoint", weight="8")

    # L2 HIT → promote to L1
    g.edge("vs_endpoint", "lakebase", style="dashed",
           label="API → promote to L1", constraint="false")

    # Full MISS → Genie
    g.edge("l2_miss", "full_miss_note", label="MISS", weight="5")
    g.edge("full_miss_note", "rate_limiter", weight="5")

    # Genie response → L1
    g.edge("genie_api", "write_l1", weight="5")
    g.edge("write_l1", "lakebase", style="dashed", label="write to L1",
           constraint="false")

    # SQL path
    g.edge("hit_resp", "hit_sql", style="dashed", label="re-execute SQL")

    g.render("scenario3-hybrid", directory=".", cleanup=True)
    print("  ✓ scenario3-hybrid.png")


if __name__ == "__main__":
    diagram_1()
    diagram_2()
    diagram_3()
    print("\nAll diagrams generated.")
