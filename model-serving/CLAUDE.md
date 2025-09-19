# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Overview

This is a Databricks model serving examples repository demonstrating how to deploy and serve various types of AI/ML models using Databricks Model Serving infrastructure. Examples include:

- **Flux.1 Dev**: Image-to-image generation models using diffusion transformers
- **Custom GLM**: Generic Linear Models with custom Python wrappers
- **Volume-based deployments**: Models stored in Unity Catalog Volumes for faster initialization

## Architecture

### Core Components

- **Model Wrapper Classes**: Python classes that implement `mlflow.pyfunc.PythonModel` to wrap external models for serving
- **Deployment Notebooks**: Jupyter notebooks that handle model registration, logging, and deployment to Databricks Model Serving
- **Unity Catalog Integration**: Models are registered in Unity Catalog with format `{catalog}.{schema}.{model_name}`

### Key Patterns

- **MLflow Integration**: All models use MLflow for logging, registration, and serving
- **GPU Memory Management**: Memory optimization techniques including model sharding, quantization, and explicit memory management
- **Volume-based Artifacts**: Large model files are stored in Databricks Unity Catalog Volumes and referenced as MLflow artifacts
- **Base64 Image Handling**: Images are converted to/from base64 for API compatibility

## Development Workflow

### Model Development

1. Create model wrapper class inheriting from `mlflow.pyfunc.PythonModel`
2. Implement `load_context()` for model initialization
3. Implement `predict()` for inference
4. Define MLflow signature with input/output schemas
5. Log and register model with MLflow

### Deployment Process

1. Use Databricks notebooks to register models
2. Configure Model Serving endpoints with appropriate GPU resources
3. Set environment variables (e.g., `FLUX_MODEL_PATH`) in serving configuration
4. Test deployed endpoints using MLflow deployment client

## Project Structure

```text
model-serving/
├── flux1-dev/
│   ├── Deploy-Flux1Dev.ipynb          # Main deployment notebook (HuggingFace download)
│   ├── Deploy-Flux1Dev-Local.ipynb    # Local deployment notebook (Volume-based)
│   ├── requirements.txt               # Python dependencies for Flux models
│   ├── test_image.jpg                 # Sample test image
│   └── result_image.jpg               # Sample output image
├── custom-glm/
│   └── Deploy-Custom-GLM.ipynb        # GLM model deployment example
└── CLAUDE.md                          # This file
```

## Key Configuration

### Model Serving Requirements

**Flux.1 Models:**
- GPU Medium (A10G x 8)
- Small 0-4 Concurrency setting
- Scale to zero enabled for development
- 20-30 minute deployment time

**Custom GLM Models:**
- CPU workload type with Small compute
- Scale to zero enabled
- Much faster deployment (~2-5 minutes)

### Required Environment Variables

- `FLUX_MODEL_PATH`: Path to model artifacts in Unity Catalog Volume
- `HUGGING_FACE_HUB_TOKEN`: For downloading models from HuggingFace (stored in Databricks secrets)

### Standard Dependencies

**Flux.1 Models:**
- `mlflow-skinny[databricks]==3.3.2` for model serving
- `transformers==4.48.0`, `torch==2.5.1`, `diffusers==0.32.2` for AI model support
- `accelerate`, `bitsandbytes==0.45.4` for GPU optimization
- `huggingface_hub==0.27.1` for model downloads

**Custom GLM Models:**
- `mlflow` for model serving
- `polars==1.30.0` for efficient data processing
- `pandas`, `numpy` for data manipulation
- `statsmodels` for GLM training

## Common Tasks

### Running Notebooks

- **Flux.1 Models**: Execute notebooks in Databricks workspace with **Serverless GPU Compute** for model registration
- **Custom GLM Models**: Can use standard CPU compute for faster iteration

### Testing Deployments

- **MLflow Client**: Use `mlflow.deployments.get_deploy_client("databricks")` to test served models programmatically
- **Direct HTTP**: Use REST API calls with proper authentication headers for endpoint testing
- **Sample Data**: Test images and datasets are included in each project directory

### Model Updates

- Re-run deployment notebooks to create new model versions
- Update serving endpoints to use new model versions
- Monitor deployment logs for successful updates

### Configuration Management

Before running notebooks, update these variables:
- `CATALOG`, `SCHEMA`, `VOLUME` for Unity Catalog storage paths
- `ENDPOINT_NAME` for serving endpoint names
- `EXPERIMENT_NAME` for MLflow experiment tracking

## Common Issues and Solutions

### Tensor Precision Mismatches

When encountering "Input type (float) and bias type (c10::Half) should be the same" errors:

- Ensure all model components (text_encoder, transformer) are loaded with consistent `torch_dtype=torch.float16`
- Pass PIL Images directly to pipelines instead of pre-converting to tensors
- Verify model artifacts were saved with consistent precision

### Model Loading Patterns

- **HuggingFace Direct**: Download models during serving initialization (Deploy-Flux1Dev.ipynb)
- **Volume-based**: Pre-download models to Unity Catalog Volumes for faster initialization (Deploy-Flux1Dev-Local.ipynb)
- Always specify `torch_dtype=torch.float16` for all model components when loading from local artifacts
