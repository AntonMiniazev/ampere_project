import matplotlib.pyplot as plt
import numpy as np

min_products = 1
max_products = 3
avg_products = 2


def scaled_beta(mean_target, low, high, alpha=2):
    # Convert target mean to normalized [0,1] scale
    mean_norm = (mean_target - low) / (high - low)

    # Fix alpha (controls concentration), calculate beta accordingly
    beta_param = alpha * (1 - mean_norm) / mean_norm

    # Generate a single beta sample
    sample = np.random.beta(alpha, beta_param)

    # Scale back to original range
    return low + sample * (high - low)


x = []
for i in range(50):
    x.append(round(scaled_beta(avg_products, min_products, max_products, 1), 0))

print(x)


# Plot the histogram of the distribution
plt.hist(x, bins=30, edgecolor="black")
plt.title("Beta-scaled Distribution (mean ~1.5, range [1, 2])")
plt.xlabel("Value")
plt.ylabel("Frequency")
plt.grid(True)
plt.show()
