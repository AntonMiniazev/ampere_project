from datetime import datetime, timedelta

import numpy as np

# Dates
today = datetime.today().date()
yesterday = today - timedelta(days=1)


def scaled_beta(mean_target: float, low: float, high: float, alpha: float = 2) -> float:
    """
    Generate a random number between low and high, biased toward mean_target using a scaled beta distribution.

    Parameters:
        mean_target (float): Desired mean value within [low, high]
        low (float): Minimum value of the output
        high (float): Maximum value of the output
        alpha (float): Shape parameter controlling distribution concentration

    Returns:
        float: Scaled random number
    """
    mean_norm = (mean_target - low) / (high - low)
    beta_param = alpha * (1 - mean_norm) / mean_norm
    sample = np.random.beta(alpha, beta_param)
    return low + sample * (high - low)
