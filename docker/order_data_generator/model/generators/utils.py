import numpy as np


def scaled_beta(mean_target: float, low: float, high: float, alpha: float = 2) -> float:
    mean_norm = (mean_target - low) / (high - low)
    beta_param = alpha * (1 - mean_norm) / mean_norm
    sample = np.random.beta(alpha, beta_param)
    return low + sample * (high - low)
