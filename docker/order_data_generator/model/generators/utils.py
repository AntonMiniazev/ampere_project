import numpy as np


def scaled_beta(mean_target: float, low: float, high: float, alpha: float = 2) -> float:
    """Sample a value from a beta distribution scaled into a target interval.

    The generator uses this helper whenever it needs realistic but bounded
    synthetic values, such as products-per-order or order-source choices. It is
    useful because it produces more natural-looking samples than a plain uniform
    distribution while still letting the caller control the expected average.
    """
    # Map beta distribution into [low, high] while targeting mean_target.
    mean_norm = (mean_target - low) / (high - low)
    beta_param = alpha * (1 - mean_norm) / mean_norm
    sample = np.random.beta(alpha, beta_param)
    return low + sample * (high - low)
