import torch

def get_device():
    """
    Returns 0 if GPU (CUDA) is available, otherwise -1 for CPU.
    This is the format expected by InsightFace ctx_id.
    """
    return 0 if torch.cuda.is_available() else -1
