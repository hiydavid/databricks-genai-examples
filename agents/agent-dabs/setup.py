from setuptools import find_packages, setup

setup(
    name="agents_on_databricks",
    version="0.1.0",
    description="Multi-agent system for Databricks with Genie agents",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    python_requires=">=3.8",
    install_requires=[
        "mlflow[databricks]==3.2.0",
        "databricks-langchain==0.6.0",
        "databricks-agents==1.4.0",
        "langgraph==0.5.4",
        "pydantic<2.12.0",
        "lark==1.2.2",
    ],
    include_package_data=True,
    package_data={
        "": ["*.yaml", "*.yml"],
    },
)
