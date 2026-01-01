
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path

plt.rcParams.update({'figure.dpi': 140, 'axes.grid': True})


def monthly_trend(df: pd.DataFrame, out_dir: Path):
    s = df.groupby('month')['Number'].count().sort_index()
    fig, ax = plt.subplots(figsize=(10,4))
    s.plot(kind='line', marker='o', ax=ax)
    ax.set_title('Monthly Incident Trend')
    ax.set_xlabel('Month')
    ax.set_ylabel('Incidents')
    fig.tight_layout()
    fig.savefig(out_dir / 'monthly_trend.png')
    plt.close(fig)


def error_family_pareto(df: pd.DataFrame, out_dir: Path):
    s = df['error_family'].value_counts()
    top = s.head(20)
    cum = top.cumsum() / top.sum() * 100

    fig, ax1 = plt.subplots(figsize=(10,5))
    top.plot(kind='bar', color='#4C78A8', ax=ax1)
    ax1.set_ylabel('Count')
    ax1.set_xlabel('Error family')
    ax2 = ax1.twinx()
    ax2.plot(range(len(top)), cum.values, color='#F58518', marker='o')
    ax2.set_ylabel('Cumulative %')
    ax1.set_title('Top Error Families (Pareto)')
    ax1.grid(axis='y', alpha=0.3)
    fig.tight_layout()
    fig.savefig(out_dir / 'error_family_pareto.png')
    plt.close(fig)


def dow_bar(df: pd.DataFrame, out_dir: Path):
    order = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
    s = df.groupby('dow')['Number'].count().reindex(order)
    fig, ax = plt.subplots(figsize=(8,4))
    s.plot(kind='bar', color='#72B7B2', ax=ax)
    ax.set_title('Incidents by Day of Week')
    ax.set_xlabel('Day')
    ax.set_ylabel('Incidents')
    fig.tight_layout()
    fig.savefig(out_dir / 'dow_bar.png')
    plt.close(fig)


def hourly_heatmap(df: pd.DataFrame, out_dir: Path):
    # Create a DOW x Hour matrix
    order = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
    pivot = (df.pivot_table(index='dow', columns='hour', values='Number', aggfunc='count')
               .reindex(order).fillna(0))
    fig, ax = plt.subplots(figsize=(10,4))
    im = ax.imshow(pivot.values, aspect='auto', cmap='YlOrRd')
    ax.set_title('Hour-of-Day Heatmap')
    ax.set_yticks(range(len(order)))
    ax.set_yticklabels(order)
    ax.set_xticks(range(24))
    ax.set_xlabel('Hour (GMT)')
    ax.set_ylabel('Day of Week')
    fig.colorbar(im, ax=ax, label='Incident count')
    fig.tight_layout()
    fig.savefig(out_dir / 'hourly_heatmap.png')
    plt.close(fig)
