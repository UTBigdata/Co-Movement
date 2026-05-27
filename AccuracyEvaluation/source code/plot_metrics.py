# -*- coding: utf-8 -*-
"""Plot Precision / Recall / F1-Score for three datasets"""
import os
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# ============================================================
# Global style (matching reference)
# ============================================================
plt.rcParams.update({
    'font.family': 'Times New Roman',
    'font.size': 7,
    'axes.labelsize': 8,
    'axes.titlesize': 8,
    'legend.fontsize': 7,
    'xtick.labelsize': 7,
    'ytick.labelsize': 7,
    'axes.unicode_minus': False,
    'axes.linewidth': 0.5,
    'xtick.major.size': 2,
    'xtick.major.width': 0.5,
    'ytick.major.size': 2,
    'ytick.major.width': 0.5,
    'legend.frameon': False,
    'figure.figsize': (8.7 / 2.54, 6 / 2.54),
    'figure.dpi': 300,
})

# ============================================================
# Custom colors
# ============================================================
color_precision = (16/255, 64/255, 96/255)
color_recall    = (230/255, 159/255, 0/255)
color_f1        = (0.55, 0.55, 0.55)

# ============================================================
# Config — paths to precision_recall_result.csv for each samples
# ============================================================

DATASETS = {
    "0.5:1": "../results/accuracy1_2/precision_recall_result.csv",
    "1:1":   "../results/accuracy1_1/precision_recall_result.csv",
    "2:1":   "../results/accuracy2_1/precision_recall_result.csv",
}

# ============================================================
# Compute metrics from results CSV
# ============================================================

def compute_metrics(path: str):
    df = pd.read_csv(path)
    tp = (df["result_type"] == "TP").sum()
    fp = (df["result_type"] == "FP").sum()
    fn = (df["result_type"] == "FN").sum()

    precision = tp / (tp + fp) * 100 if (tp + fp) > 0 else 0.0
    recall = tp / (tp + fn) * 100 if (tp + fn) > 0 else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0

    print(f"[{path}] TP={tp} FP={fp} FN={fn}  P={precision:.2f}%  R={recall:.2f}%  F1={f1:.2f}%")
    return precision, recall, f1

# ============================================================
# Main
# ============================================================

names = []
precisions = []
recalls = []
f1s = []

for name, path in DATASETS.items():
    try:
        p, r, f = compute_metrics(path)
    except FileNotFoundError:
        print(f"  ⚠ {path} not found, using placeholder 0")
        p, r, f = 0.0, 0.0, 0.0
    names.append(name)
    precisions.append(p)
    recalls.append(r)
    f1s.append(f)

# ============================================================
# Plot
# ============================================================

x = np.arange(len(names))
bar_width = 0.22
x_pos = np.arange(len(x))

fig, ax = plt.subplots()

ax.bar(x_pos - bar_width, precisions, bar_width, color=color_precision)
ax.bar(x_pos,             recalls,    bar_width, color=color_recall)
ax.bar(x_pos + bar_width, f1s,        bar_width, color=color_f1)

ax.set_xticks(x_pos)
ax.set_xticklabels(names)
ax.set_xlabel('Pos:Neg Ratio')
ax.set_ylabel('Score (%)')

ax.legend(['Precision', 'Recall', 'F1-Score'], loc='lower right', frameon=True)

plt.tight_layout()

# ============================================================
# Save (PDF + SVG)
# ============================================================
save_dir = "../results/plot"
os.makedirs(save_dir, exist_ok=True)
save_name = os.path.join(save_dir, "metrics_comparison")

plt.savefig(f"{save_name}.pdf", dpi=500, bbox_inches='tight')
plt.savefig(f"{save_name}.svg", dpi=500, bbox_inches='tight')
plt.show()
plt.close(fig)
print(f"\n[DONE] Figures saved to {save_name}.pdf / .svg")
