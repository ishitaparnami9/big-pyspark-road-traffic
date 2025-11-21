# Big Data Traffic Analysis Project

## Project Overview
This project analyzes synthetic traffic data using PySpark to identify congestion patterns, accident impacts, and vehicle type distributions. It demonstrates a full big data pipeline from data generation to visualization.

## Project Structure
*   `synthetic_data.py`: Generates synthetic datasets for roads, vehicle counts, accidents, and live traffic.
*   `traffic_congestion_analysis.py`: Performs congestion analysis using PySpark DataFrames and identifies top 5 congested roads.
*   `accident_visualization.py`: Creates an interactive dashboard to visualize accident alerts and their impact on traffic.
*   `vehicle_type_etl.py`: Implements an ETL pipeline to analyze vehicle type patterns and their distribution across roads.
*   `requirements.txt`: Lists all Python dependencies.

## Setup and Execution

1.  **Clone the repository:**
    ```bash
    git clone <your-repository-url>
    cd Traffic_Analysis_BigData
    ```

2.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

3.  **Generate Synthetic Data:**
    ```bash
    python synthetic_data.py
    ```
    This will create the necessary data files (`road_info.csv`, `vehicle_count.json`, etc.).

4.  **Run the Analyses:**
    *   Congestion Analysis: `python traffic_congestion_analysis.py`
    *   Accident Visualization: `python accident_visualization.py`
    *   Vehicle Type ETL: `python vehicle_type_etl.py`

## Results
The analyses provide insights into:
- Real-time traffic congestion hotspots.
- The correlation between accident severity and traffic flow disruption.
- Road usage patterns segmented by vehicle type (cars, bikes, trucks).

## Technologies Used
- PySpark
- Python (Pandas, Plotly)
- Jupyter Notebook / Google Colab