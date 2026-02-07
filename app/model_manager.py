from azure.storage.blob import BlobServiceClient
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
import tempfile
import shutil

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
    
    def _download_model_from_blob(self, model_name): # load from Azure blob
        if not self.connection_string:
            raise ValueError("ERROR: Azure Blob Storage connection string not found. Set AZURE_STORAGE_CONNECTION_STRING in your environment\n")
        
        try:
            model_config = self.config["models"][model_name]
            container_name = model_config["container_name"]
            blob_prefix = model_config.get("blob_prefix", '')
            temp_dir = tempfile.mkdtemp(prefix=f"{model_name}_")
            
            blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
            container_client = blob_service_client.get_container_client(container_name)
            blob_list = list(container_client.list_blobs(name_starts_with=blob_prefix))
            
            if not blob_list:
                raise ValueError(f"No files found in container '{container_name}'")
            
            for blob in blob_list:
                local_file_name = blob.name.replace(blob_prefix, '', 1) if blob_prefix else blob.name
                download_path = os.path.join(temp_dir, local_file_name)
                
                os.makedirs(os.path.dirname(download_path), exist_ok=True)
                blob_client = container_client.get_blob_client(blob.name)
                
                with open(download_path, "wb") as f:
                    f.write(blob_client.download_blob().readall())
            
            return temp_dir
                
        except Exception as e:
            raise RuntimeError(f"Failed to load model from Azure: {str(e)}") from e
    
    def load_model(self, model_name): # load to GPU/CPU memory
        model_config = self.config["models"][model_name]
        model_type = model_config["model_type"]
        temp_path = self._download_model_from_blob(model_name)
        
        try:
            if model_type == "seq2seq":
                self.tokenizers[model_name] = AutoTokenizer.from_pretrained(temp_path)
                
                if self.use_gpu:
                    self.models[model_name] = AutoModelForSeq2SeqLM.from_pretrained(
                        temp_path,
                        torch_dtype=torch.float32,  # I'm using FP32 but if it's too much load we can switch to torch.float16
                        device_map="auto",
                        low_cpu_mem_usage=False # we want to load all weights into memory so we set to False
                    ) # we could set to True and delete after use but that would use memory & the models are large
                else: # here, we're using CPU as a default but ideally we should not ever need to as it's more expensive & slower
                    self.models[model_name] = AutoModelForSeq2SeqLM.from_pretrained( 
                        temp_path,
                        torch_dtype=torch.float32,
                        low_cpu_mem_usage=False 
                    )

            elif model_type == "asr":
                self.processors[model_name] = AutoProcessor.from_pretrained(temp_path)
                
                if self.use_gpu:
                    self.models[model_name] = AutoModelForSpeechSeq2Seq.from_pretrained(
                        temp_path,
                        torch_dtype=torch.float32,
                        device_map="auto",
                        low_cpu_mem_usage=False
                    )
                else:
                    self.models[model_name] = AutoModelForSpeechSeq2Seq.from_pretrained(
                        temp_path,
                        low_cpu_mem_usage=False
                    )
            
            elif model_type == "text-to-speech":
                from transformers import AutoModel
                self.processors[model_name] = AutoProcessor.from_pretrained(temp_path)
                self.models[model_name] = AutoModel.from_pretrained(
                    temp_path,
                    device_map="auto" if self.use_gpu else None
                )
            
            shutil.rmtree(temp_path) # removing the temporary files from the ML models
            return True
            
        except Exception as e:
            raise RuntimeError(f"Failed to load model '{model_name}'.") from e

    def load_all_models(self):
        for model_name in self.config['models'].keys():
            try:
                self.load_model(model_name)
            except Exception as e:
                raise RuntimeError(f"Failed to load model '{model_name}'.") from e
    
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