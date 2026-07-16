#!/usr/bin/env python3
"""Architecture diagram with official GCP icons (diagrams + graphviz).

Regenerate any time:  pip install diagrams && python gcp_architecture.py
"""
from diagrams import Diagram, Cluster, Edge
from diagrams.gcp.analytics import BigQuery, DataCatalog
from diagrams.gcp.compute import Run
from diagrams.gcp.storage import GCS
from diagrams.gcp.devtools import Scheduler, Tasks
from diagrams.gcp.operations import Monitoring, Logging
from diagrams.gcp.ml import AIPlatform
from diagrams.gcp.security import Iam
from diagrams.gcp.analytics import Looker
from diagrams.generic.device import Tablet
from diagrams.onprem.client import Users

GRAPH = {
    "fontsize": "22",
    "fontname": "Helvetica-Bold",
    "pad": "0.4",
    "splines": "spline",
    "nodesep": "0.5",
    "ranksep": "0.85",
    "bgcolor": "white",
}
NODE = {"fontsize": "12", "fontname": "Helvetica"}
CLUS = {"fontsize": "14", "fontname": "Helvetica-Bold", "style": "rounded", "bgcolor": "#F7FAFC", "pencolor": "#9AA5AE"}

blue = Edge(color="#1C7293", penwidth="2")
gray = Edge(color="#6E7B85", penwidth="2")
mint = Edge(color="#02997A", penwidth="2")
dash = Edge(color="#065A82", style="dashed", penwidth="1.6")
red = Edge(color="#B85042", style="dashed", penwidth="1.6")

with Diagram(
    "Fridge Intelligence Data Platform - GCP",
    filename="gcp_architecture",
    outformat="png",
    show=False,
    direction="LR",
    graph_attr=GRAPH,
    node_attr=NODE,
):
    with Cluster("Edge & Device Cloud", graph_attr=CLUS):
        fridge = Tablet("Family Hub fridge\nL1-L4 diffs only")
        smartthings = Users("SmartThings Cloud\ngzip JSONL / 4-6h")

    with Cluster("Orchestration (serverless, scale-to-zero)", graph_attr=CLUS):
        sched = Scheduler("Cloud Scheduler\n0 */4 * * *")
        wf = Tasks("Cloud Workflows\npipeline DAG")

    with Cluster("Ingestion - no Pub/Sub, no Dataflow", graph_attr=CLUS):
        landing = GCS("GCS landing/\nregional")
        validator = Run("validator job\nJSON-Schema contract")
        raw = GCS("GCS raw/ archive\nlifecycle to Archive")
        quarantine = GCS("quarantine/ (DLQ)")

    with Cluster("BigQuery Lakehouse - physical billing", graph_attr=CLUS):
        bronze = BigQuery("bronze.raw_events\npartitioned + clustered\nrequire_partition_filter")
        silver = BigQuery("silver\nitem_events - fridge_items (SCD2)\nproduct_catalog")
        gold = BigQuery("gold\nhousehold_features\nreplenishment - health_scores")
        dbt = Run("dbt build job\nincremental + tests")

    with Cluster("Enrichment - resolution ladder", graph_attr=CLUS):
        enrich = Run("enrichment job\ncache -> vector -> LLM")
        llm = AIPlatform("Gemini Flash Batch\n/ self-hosted vLLM")

    with Cluster("Governance (DPDP / GDPR)", graph_attr=CLUS):
        erasure = Run("erasure job\nnightly")
        catalog = DataCatalog("Dataplex\npolicy tags - lineage")
        iam = Iam("per-dataset IAM\ngold-only analysts")

    with Cluster("Observability", graph_attr=CLUS):
        mon = Monitoring("Cloud Monitoring\n3 SLOs + alerts")
        logs = Logging("log-based metrics\nquarantine spike")

    with Cluster("Activation", graph_attr=CLUS):
        ads = Users("Samsung Ads DSP\nsegments + attribution")
        bi = Looker("Looker Studio\nanalysts")
        features = Tablet("SmartThings features\nrecipes - waste alerts")

    # main data path
    fridge >> gray >> smartthings >> gray >> landing >> blue >> validator
    validator >> blue >> raw
    validator >> Edge(color="#1C7293", penwidth="2", label="LOAD JOB (free)") >> bronze
    validator >> red >> quarantine
    bronze >> gray >> dbt >> gray >> silver >> gray >> gold

    # enrichment loop
    silver >> mint >> enrich >> mint >> llm
    enrich >> Edge(color="#02997A", penwidth="2", style="dashed", label="write-back") >> silver

    # orchestration
    sched >> dash >> wf
    wf >> dash >> validator
    wf >> dash >> dbt
    wf >> dash >> enrich

    # governance & observability touchpoints
    erasure >> red >> bronze
    erasure >> red >> raw
    catalog >> dash >> silver
    iam >> dash >> gold
    dbt >> dash >> mon
    validator >> dash >> logs

    # activation
    gold >> blue >> ads
    gold >> blue >> bi
    gold >> blue >> features
