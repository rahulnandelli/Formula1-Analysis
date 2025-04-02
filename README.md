# 🏁 Formula 1 Data Analysis

## 📌 Project Overview
This project analyzes **Formula 1 race data** using **Databricks, PySpark, Azure Data Factory, and Unity Catalog**. The goal is to extract meaningful insights into **driver performance, team strategies, and race statistics** through big data processing techniques.

## 🛠️ Technologies Used
- **PySpark**: For distributed data processing and analysis.
- **Databricks**: For executing scalable data transformations and visualizations.
- **Azure Data Factory**: For building automated ETL pipelines.
- **Unity Catalog**: For centralized metadata management and data governance.
- **GitHub**: For version control and collaboration.

## 📚 Dataset Details
The dataset consists of multiple files covering different aspects of Formula 1 racing:
- **Drivers.csv**: Driver details, career statistics, and historical records.
- **Teams.csv**: Constructors, championships, and team performance.
- **Races.csv**: Lap times, race winners, locations, and weather conditions.

## 🔍 Key Analyses & Insights
- **Driver Performance Analysis**: Evaluating driver consistency and career trends.
- **Team Strategy Insights**: Identifying constructor dominance over the years.
- **Race Data Analysis**: Studying track conditions, weather impacts, and race results.

## 🚀 Installation & Usage
### Clone the Repository
```sh
git clone https://github.com/rahulnandelli/Formula1-Analysis.git
cd Formula1-Analysis
```

### Open in Databricks
1. Upload the **notebooks/** folder to Databricks.
2. Ensure required **PySpark libraries** are installed.
3. Connect to the dataset using **Unity Catalog**.

## 🎨 Sample PySpark Query
```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Formula1 Analysis").getOrCreate()

df = spark.read.csv("data/Drivers.csv", header=True, inferSchema=True)
df.show(5)
```

## 🔗 GitHub Repository
[Formula1-Analysis](https://github.com/rahulnandelli/Formula1-Analysis)

## 📚 License
This project is **open-source** and available for educational use.

---
Feel free to **contribute** or **collaborate**! 🚀

