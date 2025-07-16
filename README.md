# Dublin Transit Stress Analysis 📊
### Project Overview
A comprehensive analysis of stress factors for Dublin’s public transit users, leveraging multi-modal data collection (GPS traces, accelerometer readings, weather data, and service schedules) to identify patterns, correlations, and actionable insights.This repository contains the code, data, and documentation for analyzing stress levels in Dublin's public transit system. The goal is to identify factors contributing to commuter stress and develop visualization and modeling tools to better understand and mitigate these stressors.

### Project Status: 
This repository is under active development. Check below for the latest roadmap and updates.

## Table of Contents

1. Introduction

2. Project Status & Roadmap

3. Data Collection

4. Repository Structure

5. Methodology (Upcoming)

6. Key Findings & Visualizations (Upcoming)

7. Installation & Usage

8. CI / Badges

9. Contributing & Collaboration

10. License

11. Contact

## Introduction

Welcome to the Dublin Transit Stress Analysis project! This repo captures our ongoing effort to quantify and understand stress factors affecting Dublin’s transit riders. As we collect and refine data, this page will evolve with new insights, visualizations, and tools.

# Motivation
Understanding stress patterns among transit users can help Dublin's transportation authorities and city planners improve service quality and commuter well-being. This project aims to:

- Collect and preprocess transit usage and environmental data.

- Analyze correlations between transit conditions (e.g., delays, weather, traffic) and reported stress levels.

- Build predictive models to estimate stress based on transit events.

- Visualize stress hotspots and trend patterns across time and locations.

## Project Status & Roadmap

| Phase              | Description                                       | ETA       |
|--------------------|---------------------------------------------------|-----------|
| Data Collection    | GPS, IMU, weather, schedule data ingestion        | Q3 2025   |
| Data Cleaning      | Preprocessing, feature engineering                | Q4 2025   |
| Exploratory EDA    | Visualizations, hotspot identification            | Q1 2026   |
| Modeling           | Predictive models for stress score                | Q2 2026   |
| Dashboard & Report | Interactive dashboard & final report publication  | Q3 2026   |

*Stay tuned for progress updates and intermediate deliverables.*

## Data Collection

Current sources: raw motion and location logs combined with external transit schedules and meteorological data.

| Source       | Description                                        | Format      | Frequency      |
|--------------|----------------------------------------------------|-------------|----------------|
| GPS & IMU    | On-vehicle sensors capturing speed and acceleration | CSV / JSON  | 1 Hz           |
| Weather API  | Temperature, humidity, precipitation               | JSON        | 1 hour         |
| Schedule     | Dublin Bus / Luas timetable & delay reports        | GTFS / JSON | Daily updates  |

## Methodology (Upcoming)

*Data cleaning, feature extraction, and statistical analyses to follow.*

## Key Findings & Visualizations (Upcoming)

*Interactive plots and summary insights will be added as analysis progresses.*

## Installation & Usage
pip install -r requirements.txt
```bash

## License

This project is licensed under the MIT License. See the LICENSE file for details.

## Contact

For questions or feedback, please contact [mkumarpani@gmail.com].

