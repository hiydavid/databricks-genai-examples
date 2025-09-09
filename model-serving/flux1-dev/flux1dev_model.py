import mlflow


class StableDiffusionImgToImg(mlflow.pyfunc.PythonModel):
    def __init__(self):
        self.pipe = None

    def load_context(self, context):
        import os

        import torch
        import transformers
        from diffusers import BitsAndBytesConfig as DiffusersBitsAndBytesConfig
        from diffusers import FluxImg2ImgPipeline, FluxTransformer2DModel
        from transformers import BitsAndBytesConfig as BitsAndBytesConfig
        from transformers import T5EncoderModel

        # transformers.utils.move_cache()
        os.environ["HUGGING_FACE_HUB_TOKEN"] = ""
        os.environ["PYTORCH_CUDA_ALLOC_CONF"] = "expandable_segments:True"

        MAX_MEMORY = {i: "24GB" for i in range(torch.cuda.device_count())}

        text_encoder_8bit = T5EncoderModel.from_pretrained(
            "black-forest-labs/FLUX.1-dev",
            subfolder="text_encoder_2",
            torch_dtype=torch.float16,
            device_map="balanced",
            max_memory=MAX_MEMORY,
        )

        transformer_8bit = FluxTransformer2DModel.from_pretrained(
            "black-forest-labs/FLUX.1-dev",
            subfolder="transformer",
            torch_dtype=torch.float16,
            device_map="balanced",
            max_memory=MAX_MEMORY,
        )

        self.flush()

        self.pipeline = FluxImg2ImgPipeline.from_pretrained(
            "black-forest-labs/FLUX.1-dev",
            text_encoder_2=text_encoder_8bit,
            transformer=transformer_8bit,
            torch_dtype=torch.float16,
            device_map="balanced",
            max_memory=MAX_MEMORY,
        )

        self.flush()

    def flush(self):
        import gc

        import torch

        gc.collect()
        torch.cuda.empty_cache()
        torch.cuda.reset_max_memory_allocated()
        torch.cuda.reset_peak_memory_stats()

    def image_to_base64(self, image):
        import base64
        from io import BytesIO

        buffered = BytesIO()
        image.save(buffered, format="JPEG")
        return base64.b64encode(buffered.getvalue()).decode("utf-8")

    def base64_to_image(self, base64_string):
        import base64
        from io import BytesIO

        import PIL
        from PIL import Image

        # Decode the base64 string
        img_data = base64.b64decode(base64_string)

        # Create a BytesIO object from the decoded data
        buffer = BytesIO(img_data)

        # Open the image using PIL
        image = Image.open(buffer)

        return image

    def predict(self, context, model_input):
        import torch
        import torchvision.transforms as T

        entry_device = torch.device(
            "cuda:0"
        )  # Or use self.pipeline.device or check hf_device_map if unsure

        prompt = model_input["prompt"][0]
        init_image = self.base64_to_image(model_input["init_image"][0])

        transform = T.Compose(
            [
                T.ToTensor(),  # (C,H,W), float in [0,1]
                T.Lambda(lambda x: x.to(torch.float16)),  # dtype match
            ]
        )
        init_image_tensor = transform(init_image).unsqueeze(
            0
        )  # Add batch dim if required
        init_image_tensor = init_image_tensor.to(entry_device)

        num_inference_steps = model_input["num_inference_steps"][0]

        strength = model_input["strength"][0]

        guidance_scale = model_input["guidance_scale"][0]

        image = self.pipeline(
            prompt=prompt,
            image=init_image_tensor,
            num_inference_steps=num_inference_steps,
            strength=strength,
            guidance_scale=guidance_scale,
        ).images[0]

        return self.image_to_base64(image)


mlflow.models.set_model(StableDiffusionImgToImg())
