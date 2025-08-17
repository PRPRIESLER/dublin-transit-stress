# 🚌 Dublin Transit Stress Analysis

**Live Demo** → [Hugging Face App](https://akii0-dublin-stress-analysis.hf.space/) (Allow some time for the application to load as it is Data Heavy)

Measuring commuter stress in Dublin’s public bus system using **real-time GTFS-RT feeds**, **weather data**, and **geospatial analytics**.  
This project collects, processes, and visualizes millions of vehicle positions to detect anomalies like vanished buses, severe delays, and stuck vehicles — all accessible through an interactive Streamlit dashboard.

---

## 📌 Motivation

As a frequent bus user in Dublin, I often experienced buses that never arrived — either delayed indefinitely or vanished from real-time boards. Commuters feel stress when services are unreliable, but it’s difficult to measure that stress directly.

This project was inspired by those experiences. The goal is to **quantify “transit stress”** in a systematic way, using open data sources, anomaly detection logic, and data visualization.

---

## 🚀 Features

- **Real-time data collection (ETL):**

  - GTFS-RT (vehicle positions, trip updates, service alerts)
  - OpenWeatherMap API (rainfall, temperature, wind)
  - Logs and nightly ETL runs

- **Data Processing:**

  - Historical + live datasets stored in **Parquet**
  - Merged per-minute **vehicle × weather records**
  - Detection of anomalies:
    - **Vanished vehicles** (gaps >30 minutes mid-service, not near final stop)
    - **Stuck buses** (minimal movement over long periods)
    - **Severely delayed buses** (arrival ~next scheduled vehicle)

- **Analytics:**

  - Stress metrics defined at **minute, route, and city levels**
  - Aggregations by day, hour, and route
  - Weather integrated into stress analysis

- **Visualization Dashboard (Streamlit):**

  - Map of live vehicle positions with stress indicators
  - Time animation of vehicle movement and stress buildup
  - Route-level aggregation view (toggle between dots and intensity view)
  - Filters for vanished vehicles, stuck buses, worst-performing routes
  - Hour + minute range sliders with autoplay

- **Deployment:**
  - App deployed on **Hugging Face Spaces (Streamlit + Docker)**
  - GitHub repository contains full pipeline and app code

---

## 🛠️ Tech Stack

- **Languages & Libraries:** Python (Pandas, Polars, NumPy, Statsmodels)
- **Geospatial:** Folium, Deck.gl (via PyDeck), OSMnx, H3
- **Visualization:** Streamlit, Plotly
- **Data Sources:**
  - GTFS-RT feeds from **Dublin Bus**
  - GTFS static files (filtered to Dublin region)
  - OpenWeatherMap API
- **Storage:** Parquet, CSV, structured directories (`data-hist`, `data-live`, `logs`)
- **Deployment:** Hugging Face Spaces (Streamlit, Docker)

---

## 📂 Repository Structure

```bash
dublin-transit-stress/
│── app/ # Streamlit dashboard components
│── collectors/ # GTFS-RT + weather collectors (ETL scripts)
│── data-hist/ # Historical datasets
│── data-live/ # Live collected datasets
│── gtfs_static/ # Filtered GTFS static reference files
│── logs/ # Logs of ETL runs
│── cache/ # Cached files
│── notebooks/ # Jupyter notebooks for analysis/prototyping
│── app.py # Main Streamlit dashboard
│── requirements.txt # Python dependencies
│── runtime.txt # Python version
```

---

## 📊 Stress Metrics Explained

- **Vanished Vehicle Rate:**  
  Vehicles with >30min gaps during service windows, not near last stop.  
  _Passenger perception: bus never came._

- **Delay Buckets:**  
  Categorized delays (5, 15, 30, 60+ minutes).  
  _Passenger perception: increasing frustration with time._

- **Stuck Buses:**  
  Vehicles detected with little/no movement over time.  
  _Passenger perception: wasted time inside the bus._

- **Weather Overlay:**  
  Stress intensified under rain, low temperature, or wind.

---

## ⚡ Results

- Collected **7+ days of GTFS-RT + weather feeds** (~millions of vehicle-minute records).
- Built stress metrics and dashboards that reveal:
  - Peak stress occurs in **morning & evening rush hours**.
  - **Certain routes** have consistently higher vanished-vehicle rates.
  - **Rainy days amplify delays and vanish rates.**

---

## 📈 Future Work

- Integrate **traffic congestion data (SCATS / other sources)**.
- Expand to **LUAS and Irish Rail datasets**.
- Add **predictive models** to forecast stress given schedule, weather, and time.
- Optimize performance for larger datasets (currently multi-GB).

---

## 🚀 Quick Start (Run Locally)

Clone the repository:

```bash
git clone https://github.com/PRPRIESLER/dublin-transit-stress.git
cd dublin-transit-stress
```
