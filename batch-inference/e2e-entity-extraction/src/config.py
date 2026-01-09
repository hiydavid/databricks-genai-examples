# Databricks notebook source
# MAGIC %md
# MAGIC # Configuration Loader
# MAGIC
# MAGIC Loads configuration from config.yaml and provides easy access to settings.

# COMMAND ----------

import yaml
import os
from pathlib import Path
from dataclasses import dataclass
from typing import Optional


def _find_config_path() -> Path:
    """Find config.yaml - works in both local and Databricks environments."""
    candidates = []

    # 1. Check if running in Databricks notebook context
    try:
        # In Databricks, get the notebook path and derive repo root
        notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()  # noqa: F821
        # notebook_path is like /Repos/user/repo/batch-inference/e2e-entity-extraction/src/config
        # We need to go up to the project root (e2e-entity-extraction)
        parts = notebook_path.split("/")
        # Find 'src' in path and go one level up
        if "src" in parts:
            src_idx = parts.index("src")
            project_root = "/Workspace" + "/".join(parts[:src_idx])
            candidates.append(Path(project_root) / "config.yaml")
    except Exception:
        pass

    # 2. Try relative to __file__ (works locally)
    try:
        here = Path(__file__).parent.parent
        candidates.append(here / "config.yaml")
    except NameError:
        pass

    # 3. Try current working directory
    candidates.append(Path.cwd() / "config.yaml")
    candidates.append(Path.cwd().parent / "config.yaml")

    # 4. Common Databricks locations
    candidates.append(Path("/Workspace/config.yaml"))

    for path in candidates:
        if path.exists():
            return path

    # Provide helpful error message
    tried = "\n  - ".join(str(p) for p in candidates)
    raise FileNotFoundError(
        f"config.yaml not found. Tried:\n  - {tried}\n"
        "Copy config.yaml.template to config.yaml and fill in values."
    )


def load_config(config_path: Optional[str] = None) -> dict:
    """Load configuration from YAML file."""
    path = Path(config_path) if config_path else _find_config_path()
    with open(path) as f:
        config = yaml.safe_load(f)

    # Resolve ${catalog} and ${schema} placeholders in volume paths
    catalog = config.get("catalog", "")
    schema = config.get("schema", "")

    if "volumes" in config:
        for key, value in config["volumes"].items():
            if isinstance(value, str):
                config["volumes"][key] = value.replace("${catalog}", catalog).replace("${schema}", schema)

    return config


@dataclass
class Config:
    """Typed configuration object."""
    catalog: str
    schema: str
    source_volume: str
    checkpoint_volume: str
    raw_table: str
    parsed_table: str
    extracted_table: str
    final_table: str
    eval_table: str
    model_name: str
    endpoint_name: str
    llm_endpoint: str
    eval_sample_rate: float

    @classmethod
    def from_yaml(cls, config_path: Optional[str] = None) -> "Config":
        """Load config from YAML file."""
        cfg = load_config(config_path)
        return cls(
            catalog=cfg["catalog"],
            schema=cfg["schema"],
            source_volume=cfg["volumes"]["source_documents"],
            checkpoint_volume=cfg["volumes"]["checkpoints"],
            raw_table=cfg["tables"]["raw_documents"],
            parsed_table=cfg["tables"]["parsed_documents"],
            extracted_table=cfg["tables"]["extracted_claims"],
            final_table=cfg["tables"]["final_claims"],
            eval_table=cfg["tables"]["evaluation"],
            model_name=cfg["model"]["name"],
            endpoint_name=cfg["model"]["endpoint_name"],
            llm_endpoint=cfg["llm"]["endpoint"],
            eval_sample_rate=cfg["evaluation"]["sample_rate"],
        )

    @property
    def raw_table_full(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.raw_table}"

    @property
    def parsed_table_full(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.parsed_table}"

    @property
    def extracted_table_full(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.extracted_table}"

    @property
    def final_table_full(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.final_table}"

    @property
    def eval_table_full(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.eval_table}"

    @property
    def registered_model_name(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.model_name}"
