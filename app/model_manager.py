from transformers import (
    AutoModelForSeq2SeqLM,
    AutoTokenizer,
    AutoModelForSpeechSeq2Seq,
    AutoProcessor
)
import os
import yaml
import torch
import warnings

class ModelManager:
    def __init__(self, config_path="config/models_config.yaml"):
        self.models = {}
        self.tokenizers = {}
        self.processors = {}
        self.config = self._load_config(config_path)
        self.connection_string = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
        self.use_gpu = torch.cuda.is_available()
        if not self.use_gpu:
            warnings.warn("No GPU device available. Defaulting to CPU.")
        self.device = "cuda" if self.use_gpu else "cpu"
    
    def _load_config(self, config_path):
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)

    def load_model(self, model_name):
        model_config = self.config["models"][model_name]
        model_type = model_config["model_type"]
        model_path = model_config["path"]

        if not os.path.isdir(model_path):
            raise RuntimeError(f"Model path does not exist: {model_path}")

        if model_type == "seq2seq":
            self.tokenizers[model_name] = AutoTokenizer.from_pretrained(model_path)
            self.models[model_name] = AutoModelForSeq2SeqLM.from_pretrained(
                model_path,
                device_map="auto" if self.use_gpu else None,
                torch_dtype=torch.float16 if self.use_gpu else torch.float32
            )

        elif model_type == "asr":
            self.processors[model_name] = AutoProcessor.from_pretrained(model_path)
            self.models[model_name] = AutoModelForSpeechSeq2Seq.from_pretrained(
                model_path,
                device_map="auto" if self.use_gpu else None,
                torch_dtype=torch.float16 if self.use_gpu else torch.float32
            )

        elif model_type == "tts":
            from transformers import AutoModel
            self.processors[model_name] = AutoProcessor.from_pretrained(model_path)
            self.models[model_name] = AutoModel.from_pretrained(
                model_path,
                device_map="auto" if self.use_gpu else None
            )
        print("Model loaded.")
        return True


    def load_all_models(self):
        for model_name in self.config['models'].keys():
            try:
                self.load_model(model_name)
            except Exception as e:
                raise RuntimeError(f"Failed to load model '{model_name}'.") from e
        print("All models loaded.")
            
    def unload_model(self, model_name):
        if model_name in self.models:
            del self.models[model_name]
        if model_name in self.tokenizers:
            del self.tokenizers[model_name]
        if model_name in self.processors:
            del self.processors[model_name]

        if self.use_gpu:
            torch.cuda.empty_cache()

        print("Model unloaded.")

        print(f"Model '{model_name}' unloaded.")

    def unload_all_models(self):
        self.models.clear()
        self.tokenizers.clear()
        self.processors.clear()

        if self.use_gpu:
            torch.cuda.empty_cache()

        print("All models unloaded.")

    
    def get_model(self, model_name):
        return self.models.get(model_name)
    
    def get_tokenizer(self, model_name):
        return self.tokenizers.get(model_name)
    
    def get_processor(self, model_name):
        return self.processors.get(model_name)
    
    def get_device_info(self):
        info = {
            "device": self.device,
            "gpu_available": self.use_gpu
        }
        
        if self.use_gpu:
            try:
                info.update({
                    "gpu_name": torch.cuda.get_device_name(0),
                    "gpu_memory_total_gb": torch.cuda.get_device_properties(0).total_memory / 1e9,
                    "gpu_memory_allocated_gb": torch.cuda.memory_allocated(0) / 1e9,
                    "gpu_memory_reserved_gb": torch.cuda.memory_reserved(0) / 1e9
                })
            except Exception:
                info["gpu_name"] = "unknown" # for unit testing - would be too slow if we actually connected Azure
                
        return info