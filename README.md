# The Three Kings: How Reddit Reacted to India's Heartbreak and Redemption

A big-data analysis of 478 GB of Reddit data, tracing how cricket fans online lived through one of the most emotional years in Indian cricket. From the 2023 World Cup final loss to the 2024 T20 World Cup win.

DATS 6450 · Big Data Analytics · Final Project · Group 9

## What This Project Is

We took the entire Reddit dataset for June 2023 to July 2024 (roughly 478 GB, 567 million posts, 3.68 billion comments) and used it to track how cricket fans online processed two events that anchored the year. India's 2023 ODI World Cup final loss to Australia in November, and their 2024 T20 World Cup win against South Africa in June.

The lens we used was three players: MS Dhoni, Virat Kohli, and Rohit Sharma. The analytical pillars were three: EDA, NLP, and ML.

The high-level question driving everything: how does an online fan community process collective heartbreak and collective redemption, and can we predict which posts ride the emotional wave?

## The 10 Research Questions

### Exploratory Data Analysis
1. Which of the three kings generates the most Reddit activity?
2. How did posting volume and sentiment change after the 2023 WC loss vs. the 2024 T20 win?
3. Which subreddits dominated during each final, and did the dominant communities shift?
4. How does engagement differ between player-specific posts and team India posts?

### Natural Language Processing
5. What is the sentiment around each player, and how does it shift during key events?
6. What are the most distinctive words used in posts about Dhoni vs. Kohli vs. Rohit?
7. What latent topics dominate during the 2023 loss period vs. the 2024 win period?

### Machine Learning
8. Can we classify a post as high-engagement (top 25% by score) from its features?
9. Can we predict the number of comments a post will receive via regression?
10. Can K-Means cluster cricket subreddits into meaningful behavioral groups?

## Repository Structure

- `README.md` — this file
- `utils.py` — shared helpers (Spark session, S3 paths, regex, time features)
- `.gitignore`
- `notebooks/` — all analysis notebooks
  - `00_data_exploration.ipynb`
  - `01_cricket_filtering.ipynb`
  - `02_eda_three_kings.ipynb`
  - `03_heartbreak_2023wc.ipynb`
  - `04_redemption_2024t20.ipynb`
  - `05_nlp_sentiment_topics.ipynb`
  - `06_ml_models.ipynb`
- `results/` — analysis outputs
  - `figures/` — 18 PNG charts
  - `tables/` — 15 CSV summary tables
- `website/`
  - `index.html` — project website with full narrative
- `presentation/`
  - `cricket_presentation.pptx` — 11-slide deck
- `setup/`
  - `spark_config.md` — Spark and AWS environment setup notes

## What Each Notebook Does

| Notebook | Purpose | Maps to |
|----------|---------|---------|
| `00_data_exploration.ipynb` | Schema validation, partition coverage, top-subreddit baselines on the raw data | Setup |
| `01_cricket_filtering.ipynb` | The big Spark job. Scans 478 GB and filters to 46 cricket subreddits. Took ~3.5 hours total. Output: 242k posts, 6.7M+ comments saved as Parquet to S3 | Setup |
| `02_eda_three_kings.ipynb` | Player engagement, monthly trends, top subreddits per player | Q1, Q4 |
| `03_heartbreak_2023wc.ipynb` | 2023 WC final loss aftermath. Daily volume, mentions by period, top posts | Q2, Q3 |
| `04_redemption_2024t20.ipynb` | 2024 T20 win aftermath plus head-to-head loss vs. win comparison | Q2, Q3 |
| `05_nlp_sentiment_topics.ipynb` | VADER sentiment, TF-IDF distinctive keywords, LDA topic modeling | Q5, Q6, Q7 |
| `06_ml_models.ipynb` | Random Forest classifier, Random Forest regressor, K-Means clustering | Q8, Q9, Q10 |

## Key Findings

**Volume vs. engagement diverge.** Rohit Sharma was mentioned in 9,910 posts (the most), but Dhoni had the highest average score per post at 99.7. Dhoni also had the most positive average sentiment at +0.108. Smaller fanbase, more devoted.

**Joy travels further than grief.** The week after the 2024 T20 win generated 22% more posts, 62% higher average score, and 17% more comments per post than the week after the 2023 WC loss. The top win post outscored the top loss post by 51%.

**Sentiment tracks the scoreboard.** Average VADER sentiment climbed monotonically from +0.053 (WC 2023 group stage) to +0.116 (T20 2024 final aftermath). The win aftermath was 2.3x more positive than the loss aftermath.

**Engagement is community-driven, not content-driven.** A Random Forest classifier trained to predict high-engagement posts found that subreddit identity accounts for ~70% of the predictive signal. Time of day, day of week, and player mentions barely contribute. Where you post matters far more than what you post about. AUC: 0.673, Accuracy: 73.86%.

**Comment count is much harder to predict.** The regression model achieved an R² of only 0.016. We report this as an honest finding: comment volume depends on conversation potential (controversy, breaking news, debate) which our metadata features don't capture.

**K-Means recovers cricket Reddit's natural structure.** With k=4, the algorithm grouped the subreddits into a mainstream giant cluster (r/Cricket alone), a viral content cluster (CricketShitpost, IndiaCricket), a fan communities cluster (RCB, MI, CSK and other team subs), and a rivalry / niche cluster (PakCricket, RR, srh). Silhouette score: 0.195.

## Infrastructure

| Component | Spec |
|-----------|------|
| Compute | AWS EC2 t3.large (2 vCPUs, 8 GB RAM, Ubuntu 22.04) |
| Storage | S3 bucket `dats6450-group9-reddit-data`, partitioned Parquet |
| Spark | 3.5.1 (local mode, `local[*]`) |
| Java | OpenJDK 11 |
| Connector | hadoop-aws 3.3.4 + aws-java-sdk-bundle 1.12.262 |
| Python | 3.12 with PySpark, pandas, scikit-learn, NLTK, matplotlib, seaborn |

The dataset lives at `s3://dats6450-group9-reddit-data/reddit/parquet/` with two top-level tables (`submissions/` and `comments/`), Hive-partitioned by `yyyy` and `mm`. After the filtering job in notebook 01, a derived `cricket_filtered/` table contains the trimmed dataset that every downstream notebook reads.

See `setup/spark_config.md` for the full Spark session configuration.

Group 9 · DATS 6450 Big Data Analytics · Spring 2026
