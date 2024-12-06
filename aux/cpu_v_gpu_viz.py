import matplotlib.pyplot as plt
import numpy as np

rows = ["1K", "10K", "100K", "1M", "10M", "40M"]
cpu_times = [20, 190, 1880, 18790, 144270, 1438730]
gpu_times = [250, 261, 430, 2196, 18735, 132425]

plt.figure(figsize=(10, 6))

plt.plot(range(len(rows)), cpu_times, "b-o", label="CPU", linewidth=2)
plt.plot(range(len(rows)), gpu_times, "g-o", label="GPU", linewidth=2)

plt.yscale("log")

plt.grid(True, which="both", ls="-", alpha=0.2)
plt.xlabel("Dataset Size")
plt.ylabel("Time (ms)")
plt.title("Join Performance: CPU vs GPU")
plt.legend()

plt.xticks(range(len(rows)), rows)

plt.grid(True, which="major", linestyle="--", alpha=0.7)
plt.grid(True, which="minor", linestyle=":", alpha=0.4)

plt.tight_layout()
plt.show()
