# 🪙 Crypto ETL Pipeline

An end-to-end automated data engineering pipeline that extracts live cryptocurrency market data from the CoinGecko API, transforms and validates it with Python, loads it into PostgreSQL, and visualises it through an interactive Plotly Dash dashboard — all orchestrated by Apache Airflow.

![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.9-017CEE?logo=apacheairflow)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-336791?logo=postgresql)
![Plotly](https://img.shields.io/badge/Plotly%20Dash-5.x-3F4F75?logo=plotly)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker)

---

## 📸 Dashboard Preview

> *(Add a screenshot or GIF of your running dashboard here)*

---

## 🏗️ Architecture

```
CoinGecko API
     │
     ▼
[ extract.py ]  ──→  Raw JSON
     │
     ▼
[ transform.py ] ──→  Cleaned & validated records
     │
     ▼
[ load.py ] ──→  PostgreSQL (crypto_db)
     │
     ▼
[ dashboard.py ] ──→  Plotly Dash on http://localhost:8050

All steps orchestrated by Apache Airflow (runs every 6 hours)
```

---

## 📁 Project Structure

```
crypto-etl-pipeline/
├── dags/
│   └── crypto_pipeline_dag.py   # Airflow DAG (Extract → Transform → Load → Notify)
├── etl/
│   ├── extract.py               # Fetches data from CoinGecko API
│   ├── transform.py             # Cleans and validates raw data
│   └── load.py                  # Inserts records into PostgreSQL
├── visualisation/
│   └── dashboard.py             # Interactive Plotly Dash dashboard
├── sql/
│   └── create_tables.sql        # PostgreSQL schema + indexes + view
├── .env.example                 # Environment variable template
├── requirements.txt
├── docker-compose.yml           # Spins up Airflow + PostgreSQL + Dashboard
└── README.md
```

---

## 🚀 Getting Started

### Prerequisites
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running
- Git

### 1. Clone the repository
```bash
git clone https://github.com/YOUR_USERNAME/crypto-etl-pipeline.git
cd crypto-etl-pipeline
```

### 2. Set up environment variables
```bash
cp .env.example .env
# Edit .env with your preferred values
```

### 3. Start all services with Docker
```bash
docker-compose up --build
```

This will start:
| Service | URL |
|---|---|
| Airflow Webserver | http://localhost:8080 |
| Plotly Dashboard | http://localhost:8050 |
| PostgreSQL | localhost:5432 |

### 4. Trigger the pipeline manually
1. Open Airflow at http://localhost:8080 (user: `admin`, pass: `admin`)
2. Find the `crypto_etl_pipeline` DAG
3. Click the ▶️ **Trigger DAG** button
4. Watch each task turn green: `extract → transform → load → notify`

### 5. View the dashboard
Open http://localhost:8050 to see live charts once data has loaded.

---

## ⚙️ Running Without Docker

```bash
# Install dependencies
pip install -r requirements.txt

# Set up your .env file
cp .env.example .env

# Initialise the database
python etl/load.py

# Run the pipeline manually
python -c "
from etl import fetch_crypto_data, transform_crypto_data, insert_crypto_records
raw = fetch_crypto_data()
clean = transform_crypto_data(raw)
insert_crypto_records(clean)
print('Pipeline complete!')
"

# Launch the dashboard
python visualisation/dashboard.py
```

---

## 📊 Dashboard Features

| Chart | Description |
|---|---|
| Price History | Line chart of price over time per coin |
| 24h Change Bar | Horizontal bar showing gainers vs losers |
| Market Cap Pie | Share of market cap across tracked coins |
| Volume Chart | 24h trading volume comparison |
| KPI Cards | Live price, market cap, 24h change, last updated |

---

## 🪙 Tracked Coins

Bitcoin · Ethereum · Solana · Cardano · XRP · Dogecoin · Polkadot · Chainlink

> Easily extend in `etl/extract.py` → `COINS_TO_TRACK` list.

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| Orchestration | Apache Airflow 2.9 |
| Extraction | Python `requests` + CoinGecko API |
| Transformation | Python (Pandas, dataclasses) |
| Storage | PostgreSQL 15 |
| Visualisation | Plotly Dash |
| Containerisation | Docker Compose |

---

## 💡 What I Learned

- Designing and building a production-style ETL pipeline from scratch
- Using Apache Airflow XCom to pass data between tasks
- Writing efficient bulk inserts with `psycopg2.extras.execute_batch`
- Building reactive dashboards with Plotly Dash callbacks
- Containerising a multi-service application with Docker Compose

---

## 🔮 Future Improvements

- [ ] Add email/Slack alerts on pipeline failure
- [ ] Expand to 50+ coins using pagination
- [ ] Add historical backfill using CoinGecko's `/history` endpoint
- [ ] Deploy dashboard to Render or Railway
- [ ] Add unit tests with `pytest`

---

## 📄 License

MIT License — feel free to fork and build on this project.

---

*Built by [Lamla](https://github.com/Lami14) · Data Engineering Portfolio Project*

