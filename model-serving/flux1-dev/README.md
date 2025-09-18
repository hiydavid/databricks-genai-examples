# Flux.1 Dev Model Serving

Advanced text-to-image generation using Flux.1 Dev model on Databricks Model Serving infrastructure. This project demonstrates how to deploy and serve the Black Forest Labs Flux.1 Dev model for high-quality image generation from text prompts.

## Overview

Flux.1 Dev is a state-of-the-art diffusion model that generates high-quality images from text descriptions. This implementation provides:

- **MLflow Integration**: Model registration and serving through MLflow
- **GPU Optimization**: Efficient GPU memory management with model sharding
- **Unity Catalog**: Seamless integration with Databricks Unity Catalog
- **REST API**: Production-ready endpoints for inference

## Files

- `Deploy-Flux1Dev.ipynb` - Main deployment notebook (downloads model from HuggingFace)
- `Deploy-Flux1Dev-Local.ipynb` - Local deployment notebook (uses pre-downloaded model from Volume)
- `requirements.txt` - Python dependencies for model serving
- `test_image.jpg` - Sample input image for testing
- `result_image.jpg` - Example output from the model

## Prerequisites

- Databricks workspace with Model Serving enabled
- GPU Medium compute (A10G x 8) for deployment
- HuggingFace account with API token
- Unity Catalog configured
