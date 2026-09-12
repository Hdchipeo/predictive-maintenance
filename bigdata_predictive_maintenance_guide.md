# Hướng dẫn triển khai hệ thống Big Data Predictive Maintenance

## 1. Tổng quan

### 1.1. Tên đề tài

**Xây dựng hệ thống Big Data dự đoán thời gian sống còn của thiết bị
công nghiệp từ dữ liệu cảm biến streaming**

Tên tiếng Anh đề xuất:

> **Real-Time Big Data Predictive Maintenance System for Remaining
> Useful Life Prediction**

### 1.2. Mục tiêu

Xây dựng một hệ thống có khả năng:

1.  Tiếp nhận dữ liệu cảm biến theo dạng streaming.
2.  Mô phỏng dữ liệu cảm biến từ dataset NASA C-MAPSS.
3.  Đưa dữ liệu vào Apache Kafka.
4.  Xử lý dữ liệu phân tán bằng Apache Spark.
5.  Feature Engineering cho dữ liệu chuỗi thời gian.
6.  Dự đoán Remaining Useful Life (RUL).
7.  So sánh Random Forest và XGBoost.
8.  Lưu dữ liệu cảm biến và kết quả dự đoán vào InfluxDB.
9.  Trực quan hóa tình trạng thiết bị bằng Grafana.
10. Đánh giá cả độ chính xác ML và hiệu năng Big Data: throughput,
    latency, CPU/RAM và khả năng scale.

------------------------------------------------------------------------

# 2. Bài toán cần giải quyết

## 2.1. Predictive Maintenance

Trong bảo trì truyền thống, thiết bị thường được bảo trì:

-   theo lịch cố định;
-   sau khi xảy ra lỗi;
-   hoặc dựa vào kinh nghiệm kỹ thuật viên.

Predictive Maintenance sử dụng dữ liệu cảm biến để phát hiện degradation
và dự đoán thời điểm thiết bị cần bảo trì.

Mục tiêu của đồ án:

``` text
Sensor Data
    ↓
Degradation Analysis
    ↓
RUL Prediction
    ↓
Health Assessment
    ↓
Maintenance Alert
```

## 2.2. RUL là gì?

**Remaining Useful Life (RUL)** là thời gian hoặc số chu kỳ còn lại
trước khi thiết bị đạt trạng thái failure.

Ví dụ:

``` text
Engine #17
Current cycle = 143
Predicted RUL = 36 cycles
```

Có thể chuyển RUL thành trạng thái:

``` text
RUL > 100       → HEALTHY
50 < RUL ≤ 100  → NORMAL
20 < RUL ≤ 50   → WARNING
RUL ≤ 20        → CRITICAL
```

Các ngưỡng trên chỉ là ngưỡng minh họa của hệ thống; khi nghiên cứu cần
hiệu chỉnh dựa trên bài toán thực tế.

------------------------------------------------------------------------

# 3. Dataset NASA C-MAPSS

## 3.1. Dataset

Sử dụng:

**NASA C-MAPSS Jet Engine Simulated Data**

Dataset mô phỏng dữ liệu cảm biến của các động cơ phản lực trong quá
trình hoạt động và degradation.

Dataset có nhiều subset:

  Dataset     Train engines   Operating conditions   Fault modes
  --------- --------------- ---------------------- -------------
  FD001                 100                      1             1
  FD002                 260                      6             1
  FD003                 100                      1             2
  FD004                 249                      6             2

### Khuyến nghị

Bắt đầu với:

``` text
FD001
```

Sau khi pipeline chạy ổn định, mở rộng sang:

``` text
FD002
```

FD002 phù hợp để nghiên cứu tác động của nhiều operating conditions.

------------------------------------------------------------------------

# 4. Kiến trúc hệ thống

## 4.1. Kiến trúc tổng thể

``` text
                         NASA C-MAPSS
                              │
                              ▼
                    ┌──────────────────┐
                    │ Python Producer   │
                    │ Data Replay       │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │      Kafka       │
                    │ Sensor Streaming │
                    └────────┬─────────┘
                             │
                             ▼
              ┌────────────────────────────┐
              │ Apache Spark               │
              │ Structured Streaming       │
              │                            │
              │ - Parse                    │
              │ - Clean                    │
              │ - Feature Engineering      │
              │ - ML Inference             │
              └─────────────┬──────────────┘
                            │
                    ┌───────┴────────┐
                    │                │
                    ▼                ▼
             ┌────────────┐   ┌────────────┐
             │ Random     │   │ XGBoost    │
             │ Forest     │   │ Regression │
             └─────┬──────┘   └─────┬──────┘
                   └────────┬────────┘
                            ▼
                      RUL Prediction
                            │
                            ▼
                    ┌───────────────┐
                    │   InfluxDB    │
                    │ Time Series DB│
                    └───────┬───────┘
                            │
                            ▼
                    ┌───────────────┐
                    │    Grafana    │
                    │   Dashboard   │
                    └───────────────┘
```

## 4.2. Vai trò từng thành phần

  Thành phần       Vai trò
  ---------------- -------------------------------------
  NASA C-MAPSS     Dataset huấn luyện và đánh giá
  Python           Replay dữ liệu và Kafka Producer
  Kafka            Message broker / streaming platform
  Spark            Distributed processing + streaming
  MLlib            Random Forest và các pipeline ML
  XGBoost          RUL regression model
  InfluxDB         Time-series storage
  Grafana          Dashboard
  Docker Compose   Quản lý môi trường triển khai

------------------------------------------------------------------------

# 5. Tech Stack

Khuyến nghị:

``` text
Python 3.x
Apache Kafka
Apache Spark
PySpark
XGBoost
InfluxDB
Grafana
Docker / Docker Compose
Pandas
NumPy
scikit-learn
```

## 5.1. Vì sao Docker Compose?

Đồ án có nhiều service:

``` text
Kafka
Spark
InfluxDB
Grafana
```

Cài thủ công trên máy rất dễ xảy ra lỗi version và dependency.

Docker Compose giúp:

``` text
docker compose up -d
```

và toàn bộ hệ thống được khởi động cùng nhau.

------------------------------------------------------------------------

# 6. Cấu trúc project

Khuyến nghị cấu trúc:

``` text
predictive-maintenance/
│
├── docker/
│   ├── docker-compose.yml
│   ├── spark/
│   └── kafka/
│
├── data/
│   ├── raw/
│   │   ├── train_FD001.txt
│   │   ├── test_FD001.txt
│   │   └── RUL_FD001.txt
│   │
│   └── processed/
│
├── producer/
│   ├── kafka_producer.py
│   └── replay.py
│
├── preprocessing/
│   ├── create_rul.py
│   ├── clean.py
│   └── feature_engineering.py
│
├── training/
│   ├── train_random_forest.py
│   ├── train_xgboost.py
│   └── evaluate.py
│
├── streaming/
│   ├── spark_streaming.py
│   ├── inference.py
│   └── sink_influxdb.py
│
├── dashboard/
│   └── grafana/
│
├── notebooks/
│   ├── 01_eda.ipynb
│   ├── 02_feature_engineering.ipynb
│   └── 03_model_comparison.ipynb
│
├── requirements.txt
└── README.md
```

------------------------------------------------------------------------

# 7. Giai đoạn 1 --- Khám phá dữ liệu

Đầu tiên cần hiểu dataset trước khi đưa Kafka vào.

Các thông tin cần phân tích:

``` text
Number of engines
Number of cycles
Number of sensors
Missing values
Constant sensors
Sensor distributions
Sensor correlation
Degradation trend
```

Ví dụ đọc dataset:

``` python
import pandas as pd

columns = [
    "engine_id",
    "cycle",
    "setting_1",
    "setting_2",
    "setting_3",
]

columns += [f"sensor_{i}" for i in range(1, 22)]

df = pd.read_csv(
    "data/raw/train_FD001.txt",
    sep=r"\s+",
    header=None
)

df = df.iloc[:, :len(columns)]
df.columns = columns

print(df.head())
print(df.shape)
print(df.isna().sum())
```

------------------------------------------------------------------------

# 8. Giai đoạn 2 --- Tạo RUL Label

## 8.1. Công thức

Với mỗi engine:

``` text
RUL = max_cycle(engine) - current_cycle
```

Ví dụ:

``` text
Engine 1
max_cycle = 192

cycle = 1
RUL = 191

cycle = 100
RUL = 92

cycle = 192
RUL = 0
```

## 8.2. Python

``` python
max_cycle = df.groupby("engine_id")["cycle"].transform("max")

df["RUL"] = max_cycle - df["cycle"]

print(df[["engine_id", "cycle", "RUL"]].head())
```

------------------------------------------------------------------------

# 9. Giai đoạn 3 --- Data Cleaning

Cần kiểm tra:

``` text
Missing values
Duplicate records
Invalid values
Constant sensors
Outliers
Sensor scaling
```

## 9.1. Loại sensor không có thông tin

Một số sensor có thể gần như không thay đổi.

Kiểm tra:

``` python
sensor_columns = [
    c for c in df.columns
    if c.startswith("sensor_")
]

variance = df[sensor_columns].var()

print(variance.sort_values())
```

Có thể loại các sensor có variance quá thấp.

Không nên xóa sensor một cách tùy tiện; cần ghi lại tiêu chí trong báo
cáo.

------------------------------------------------------------------------

# 10. Giai đoạn 4 --- Feature Engineering

Đây là phần rất quan trọng của bài toán.

## 10.1. Moving Average

``` python
df["sensor_2_ma_10"] = (
    df.groupby("engine_id")["sensor_2"]
      .transform(lambda x: x.rolling(10, min_periods=1).mean())
)
```

## 10.2. Rolling Standard Deviation

``` python
df["sensor_2_std_10"] = (
    df.groupby("engine_id")["sensor_2"]
      .transform(lambda x: x.rolling(10, min_periods=1).std())
)
```

## 10.3. Trend

``` python
df["sensor_2_delta_10"] = (
    df.groupby("engine_id")["sensor_2"]
      .transform(lambda x: x.diff(10))
)
```

## 10.4. Feature Set

Có thể xây dựng:

``` text
Raw sensor
Moving average
Rolling standard deviation
Minimum
Maximum
Delta
Rate of change
```

------------------------------------------------------------------------

# 11. Sliding Window

Dữ liệu là time series nên có thể sử dụng sliding window.

Ví dụ:

``` text
window = 20 cycles
```

Một sample:

``` text
t-19
t-18
t-17
...
t-1
t
```

Mục tiêu:

``` text
Predict RUL(t)
```

Có thể thử:

``` text
Window = 5
Window = 10
Window = 20
Window = 30
```

Đây là một thí nghiệm nghiên cứu tốt.

------------------------------------------------------------------------

# 12. Giai đoạn 5 --- Train Random Forest

Random Forest được sử dụng làm baseline model.

Pipeline:

``` text
Features
   ↓
Train/Test Split
   ↓
Random Forest Regressor
   ↓
RUL
```

Ví dụ:

``` python
from sklearn.ensemble import RandomForestRegressor

model = RandomForestRegressor(
    n_estimators=200,
    max_depth=None,
    random_state=42,
    n_jobs=-1
)

model.fit(X_train, y_train)

prediction = model.predict(X_test)
```

Metrics:

``` python
from sklearn.metrics import mean_absolute_error
from sklearn.metrics import mean_squared_error
from sklearn.metrics import r2_score

mae = mean_absolute_error(y_test, prediction)
rmse = mean_squared_error(
    y_test,
    prediction
) ** 0.5
r2 = r2_score(y_test, prediction)

print("MAE:", mae)
print("RMSE:", rmse)
print("R2:", r2)
```

------------------------------------------------------------------------

# 13. Giai đoạn 6 --- Train XGBoost

XGBoost được sử dụng làm model chính để so sánh với Random Forest.

Ví dụ:

``` python
from xgboost import XGBRegressor

model = XGBRegressor(
    n_estimators=500,
    max_depth=6,
    learning_rate=0.05,
    subsample=0.8,
    colsample_bytree=0.8,
    objective="reg:squarederror",
    random_state=42
)

model.fit(X_train, y_train)

prediction = model.predict(X_test)
```

Sau đó đánh giá:

``` text
MAE
RMSE
R²
Training time
Inference time
```

------------------------------------------------------------------------

# 14. So sánh Model

Tạo bảng:

  --------------------------------------------------------------------------
  Model              MAE         RMSE           R²     Training    Inference
                                                           Time         Time
  --------- ------------ ------------ ------------ ------------ ------------
  Random         đo thực      đo thực      đo thực           đo           đo
  Forest          nghiệm       nghiệm       nghiệm              

  XGBoost        đo thực      đo thực      đo thực           đo           đo
                  nghiệm       nghiệm       nghiệm              
  --------------------------------------------------------------------------

Không nên đưa số liệu giả vào báo cáo. Các giá trị phải được đo từ hệ
thống thực tế.

------------------------------------------------------------------------

# 15. Giai đoạn 7 --- Kafka Streaming

## 15.1. Kafka Topic

Tạo topic:

``` text
engine_sensor_raw
```

Có thể mở rộng:

``` text
engine_sensor_processed
rul_prediction
engine_alert
```

## 15.2. Sensor message

Định dạng JSON:

``` json
{
  "engine_id": 17,
  "cycle": 143,
  "sensor_2": 642.12,
  "sensor_3": 1589.42,
  "sensor_4": 1400.31
}
```

------------------------------------------------------------------------

# 16. Kafka Producer

Producer đọc dữ liệu NASA và replay thành streaming.

Ví dụ:

``` python
import json
import time
import pandas as pd
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

for _, row in df.iterrows():

    message = {
        "engine_id": int(row["engine_id"]),
        "cycle": int(row["cycle"]),
        "sensor_2": float(row["sensor_2"]),
        "sensor_3": float(row["sensor_3"]),
        "sensor_4": float(row["sensor_4"])
    }

    producer.send(
        "engine_sensor_raw",
        value=message
    )

    time.sleep(0.1)

producer.flush()
```

`time.sleep()` ở đây dùng để mô phỏng tốc độ sensor.

------------------------------------------------------------------------

# 17. Replay Rate

Có thể thử nhiều tốc độ:

``` text
10 msg/s
100 msg/s
500 msg/s
1000 msg/s
5000 msg/s
```

Mục đích không phải chứng minh NASA dataset thật sự phát sinh ở tốc độ
đó, mà để **stress-test pipeline**.

------------------------------------------------------------------------

# 18. Giai đoạn 8 --- Spark Structured Streaming

Spark đọc Kafka:

``` text
Kafka
  ↓
Spark Structured Streaming
```

Schema:

``` python
from pyspark.sql.types import *

schema = StructType([
    StructField("engine_id", IntegerType()),
    StructField("cycle", IntegerType()),
    StructField("sensor_2", DoubleType()),
    StructField("sensor_3", DoubleType()),
    StructField("sensor_4", DoubleType())
])
```

Đọc Kafka:

``` python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("PredictiveMaintenance")
    .getOrCreate()
)

raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "engine_sensor_raw")
    .option("startingOffsets", "latest")
    .load()
)
```

Parse JSON:

``` python
from pyspark.sql.functions import col
from pyspark.sql.functions import from_json

events = (
    raw.select(
        from_json(
            col("value").cast("string"),
            schema
        ).alias("data")
    )
    .select("data.*")
)
```

------------------------------------------------------------------------

# 19. Streaming Feature Engineering

Có thể sử dụng window theo engine/cycle.

Ví dụ ý tưởng:

``` text
Engine 17
      │
      ├── cycle 140
      ├── cycle 141
      ├── cycle 142
      ├── cycle 143
      └── cycle 144
             │
             ▼
       Feature Window
             │
             ▼
         ML Model
```

Trong Spark Structured Streaming, cần đặc biệt chú ý:

-   event time;
-   watermark;
-   stateful processing;
-   late-arriving data;
-   checkpoint;
-   output mode.

------------------------------------------------------------------------

# 20. Giai đoạn 9 --- ML Inference trong Streaming

Model được train offline trước:

``` text
Historical Data
      ↓
Training
      ↓
Model
```

Sau đó streaming pipeline chỉ thực hiện inference:

``` text
Kafka
 ↓
Spark
 ↓
Feature Engineering
 ↓
Loaded Model
 ↓
RUL Prediction
```

Đây là kiến trúc thực tế hơn việc train model liên tục trên từng
message.

------------------------------------------------------------------------

# 21. Offline Training và Online Inference

Nên tách hai pipeline:

## Training pipeline

``` text
NASA Dataset
    ↓
Cleaning
    ↓
Feature Engineering
    ↓
Training
    ↓
Model
    ↓
Model Artifact
```

## Streaming inference

``` text
Sensor
   ↓
Kafka
   ↓
Spark
   ↓
Feature Engineering
   ↓
Model
   ↓
RUL
```

Đây là một điểm kiến trúc quan trọng.

------------------------------------------------------------------------

# 22. Model Versioning

Nên lưu:

``` text
model_name
model_version
training_date
dataset
features
parameters
metrics
```

Ví dụ:

``` text
xgboost_rul_v1
dataset = FD001
window = 20
RMSE = <measured value>
```

Có thể lưu metadata trong JSON:

``` json
{
  "model": "xgboost",
  "version": "1.0",
  "dataset": "FD001",
  "window": 20,
  "features": 84
}
```

------------------------------------------------------------------------

# 23. Giai đoạn 10 --- InfluxDB

InfluxDB dùng để lưu dữ liệu time series.

Schema logic:

``` text
measurement:
    engine_health

tags:
    engine_id
    status

fields:
    cycle
    predicted_rul
    health_score
```

Ví dụ record:

``` text
engine_health
engine_id=17
status=WARNING
cycle=143
predicted_rul=36
health_score=72
```

Không nên lưu mọi thứ dưới dạng tag vì cardinality cao có thể làm
database kém hiệu quả.

------------------------------------------------------------------------

# 24. Giai đoạn 11 --- Grafana

Dashboard nên có ít nhất 5 panel.

## Panel 1 --- Engine Health

``` text
Engine #17

Status:
WARNING

RUL:
36 cycles
```

## Panel 2 --- RUL Trend

``` text
RUL
│\
│ \
│  \
│   \
│    \____
└──────────── Cycle
```

## Panel 3 --- Sensor Trend

Ví dụ:

``` text
Temperature
Pressure
Vibration
```

## Panel 4 --- Health Score

``` text
Health Score = 72%
```

## Panel 5 --- Alert

``` text
CRITICAL ENGINE
Engine #23
RUL = 12 cycles
```

------------------------------------------------------------------------

# 25. Health Score

Có thể xây dựng một health score đơn giản:

``` text
Health Score = 100 × min(RUL / RUL_reference, 1)
```

Ví dụ nếu:

``` text
RUL_reference = 100
RUL = 72
```

thì:

``` text
Health Score = 72%
```

Đây là chỉ số trực quan hóa, không phải một tiêu chuẩn công nghiệp.
Trong báo cáo cần phân biệt rõ **prediction target RUL** và **derived
visualization metric Health Score**.

------------------------------------------------------------------------

# 26. Alert Engine

Có thể tạo rule:

``` text
RUL > 50
    NORMAL

20 < RUL <= 50
    WARNING

RUL <= 20
    CRITICAL
```

Luồng:

``` text
RUL Prediction
      ↓
Threshold
      ↓
Alert
      ↓
Kafka / Database
      ↓
Grafana
```

Có thể mở rộng thành:

``` text
CRITICAL
   ↓
Email / Telegram / Webhook
```

------------------------------------------------------------------------

# 27. Big Data Performance Evaluation

Đây là phần bắt buộc nếu muốn đồ án có chất Big Data.

Không chỉ đo model accuracy.

Cần đo:

### Throughput

Số message xử lý mỗi giây:

``` text
messages / second
```

### Latency

Thời gian từ:

``` text
Kafka ingestion
      ↓
Spark processing
      ↓
Prediction
      ↓
InfluxDB
```

### Resource Usage

Đo:

``` text
CPU
RAM
Disk I/O
Network
```

------------------------------------------------------------------------

# 28. Scalability Experiment

Thiết kế thí nghiệm:

``` text
Test A:
100 msg/s

Test B:
500 msg/s

Test C:
1000 msg/s

Test D:
5000 msg/s
```

Ghi:

``` text
Throughput
Latency
CPU
RAM
Dropped messages
```

Bảng:

    Input rate   Throughput   Avg latency   CPU   RAM
  ------------ ------------ ------------- ----- -----
     100 msg/s           đo            đo    đo    đo
     500 msg/s           đo            đo    đo    đo
    1000 msg/s           đo            đo    đo    đo
    5000 msg/s           đo            đo    đo    đo

------------------------------------------------------------------------

# 29. Experiment về Feature Engineering

So sánh:

``` text
Experiment A
Raw sensors
```

với:

``` text
Experiment B
Raw + rolling mean + std + trend
```

Metrics:

``` text
MAE
RMSE
R²
```

Mục tiêu:

> Feature Engineering có cải thiện RUL prediction hay không?

------------------------------------------------------------------------

# 30. Experiment về Sliding Window

Thử:

``` text
Window = 5
Window = 10
Window = 20
Window = 30
```

So sánh:

    Window   MAE   RMSE   Training time
  -------- ----- ------ ---------------
         5    đo     đo              đo
        10    đo     đo              đo
        20    đo     đo              đo
        30    đo     đo              đo

------------------------------------------------------------------------

# 31. Experiment Random Forest vs XGBoost

Đây là thí nghiệm chính.

``` text
Same Dataset
      │
      ├──────────────┐
      ▼              ▼
Random Forest     XGBoost
      │              │
      ▼              ▼
    Metrics        Metrics
      │              │
      └──────┬───────┘
             ▼
        Comparison
```

Đảm bảo hai model sử dụng:

-   cùng training set;
-   cùng validation/test strategy;
-   cùng feature set;
-   cùng target.

------------------------------------------------------------------------

# 32. Data Leakage --- cực kỳ quan trọng

Với time-series, không nên random split một cách tùy tiện.

Ví dụ nguy hiểm:

``` python
train_test_split(df)
```

Nếu các cycle của cùng một engine xuất hiện ở cả train và test, model có
thể nhìn thấy pattern của cùng một engine ở cả hai tập.

Nên ưu tiên split theo:

``` text
Engine
```

hoặc sử dụng đúng train/test protocol của C-MAPSS.

Đây là vấn đề cần trình bày rõ trong báo cáo vì nó ảnh hưởng trực tiếp
đến độ tin cậy của kết quả.

------------------------------------------------------------------------

# 33. Kiến trúc triển khai thực tế hơn

Sau khi MVP chạy được:

``` text
             ┌──────────────┐
             │ IoT Sensors  │
             └──────┬───────┘
                    │
                    ▼
             ┌──────────────┐
             │ IoT Gateway  │
             └──────┬───────┘
                    │
                    ▼
              ┌───────────┐
              │   Kafka   │
              └─────┬─────┘
                    │
          ┌─────────┴─────────┐
          ▼                   ▼
   ┌────────────┐      ┌────────────┐
   │ Spark      │      │ Raw Storage│
   │ Streaming  │      │            │
   └─────┬──────┘      └────────────┘
         │
         ▼
   ┌────────────┐
   │ ML Model   │
   └─────┬──────┘
         │
         ▼
   ┌────────────┐
   │ InfluxDB   │
   └─────┬──────┘
         │
         ▼
   ┌────────────┐
   │ Grafana    │
   └────────────┘
```

------------------------------------------------------------------------

# 34. Roadmap triển khai

## Phase 1 --- EDA

``` text
Dataset
 ↓
EDA
 ↓
RUL
 ↓
Sensor analysis
```

## Phase 2 --- ML baseline

``` text
Feature Engineering
 ↓
Random Forest
 ↓
XGBoost
 ↓
Evaluation
```

## Phase 3 --- Kafka

``` text
NASA
 ↓
Python Producer
 ↓
Kafka
```

## Phase 4 --- Spark

``` text
Kafka
 ↓
Spark Structured Streaming
 ↓
Processing
```

## Phase 5 --- Online inference

``` text
Spark
 ↓
Model
 ↓
RUL
```

## Phase 6 --- Storage

``` text
RUL
 ↓
InfluxDB
```

## Phase 7 --- Visualization

``` text
InfluxDB
 ↓
Grafana
```

## Phase 8 --- Benchmark

``` text
Throughput
Latency
CPU
RAM
Scalability
```

------------------------------------------------------------------------

# 35. MVP nên hoàn thành trước

Nếu thời gian hạn chế, chỉ cần hoàn thành:

``` text
NASA FD001
      ↓
Python
      ↓
Kafka
      ↓
Spark Structured Streaming
      ↓
XGBoost
      ↓
InfluxDB
      ↓
Grafana
```

Sau đó mới thêm:

``` text
Random Forest
Feature Engineering
Sliding Window
Performance Benchmark
Alerts
FD002
```

Không nên triển khai Kafka Cluster + Spark Cluster ngay từ đầu. Hãy làm
hệ thống single-node chạy end-to-end trước.

------------------------------------------------------------------------

# 36. Các mốc demo

## Demo 1

Hiển thị:

``` text
NASA Dataset
↓
RUL
```

## Demo 2

``` text
Python Producer
↓
Kafka
```

Kiểm tra message trên Kafka.

## Demo 3

``` text
Kafka
↓
Spark
↓
Prediction
```

## Demo 4

``` text
Prediction
↓
InfluxDB
```

## Demo 5

``` text
InfluxDB
↓
Grafana
```

## Demo cuối

Chạy toàn bộ:

``` text
Sensor Replay
   ↓
Kafka
   ↓
Spark
   ↓
XGBoost
   ↓
RUL
   ↓
InfluxDB
   ↓
Grafana
```

Sau đó tăng tốc độ producer và quan sát hệ thống.

------------------------------------------------------------------------

# 37. Kịch bản demo cuối cùng

Ví dụ:

``` text
Start system
```

Producer bắt đầu replay Engine #17.

Grafana:

``` text
Engine #17
RUL: 120
Status: HEALTHY
```

Sau một thời gian:

``` text
Engine #17
RUL: 72
Status: NORMAL
```

Tiếp tục:

``` text
Engine #17
RUL: 37
Status: WARNING
```

Cuối cùng:

``` text
Engine #17
RUL: 12
Status: CRITICAL
```

Dashboard đồng thời hiển thị sensor degradation.

Đây là phần demo rất trực quan.

------------------------------------------------------------------------

# 38. Những vấn đề kỹ thuật cần nghiên cứu

## Kafka

Keywords:

``` text
Producer
Consumer
Topic
Partition
Offset
Consumer Group
Replication
Retention
```

## Spark

Keywords:

``` text
Spark DataFrame
Spark MLlib
Structured Streaming
Watermark
Checkpoint
Window
Stateful Processing
```

## Machine Learning

Keywords:

``` text
Regression
Random Forest
XGBoost
Feature Engineering
Hyperparameter Tuning
Cross Validation
MAE
RMSE
R²
```

## Time Series

Keywords:

``` text
Sliding Window
Rolling Mean
Rolling STD
Trend
Degradation
Temporal Dependency
```

## Big Data

Keywords:

``` text
Throughput
Latency
Scalability
Distributed Processing
Fault Tolerance
Data Pipeline
Stream Processing
```

------------------------------------------------------------------------

# 39. Các câu hỏi nghiên cứu

Đề tài có thể xây dựng thành các Research Questions:

### RQ1

> XGBoost hay Random Forest cho kết quả dự đoán RUL tốt hơn trên NASA
> C-MAPSS?

### RQ2

> Feature Engineering có cải thiện độ chính xác dự đoán RUL không?

### RQ3

> Sliding Window size ảnh hưởng như thế nào đến hiệu năng mô hình?

### RQ4

> Apache Spark Structured Streaming có đáp ứng được near-real-time
> inference không?

### RQ5

> Throughput và latency thay đổi như thế nào khi tăng tốc độ dữ liệu
> sensor?

### RQ6

> Hệ thống có thể scale như thế nào khi số lượng engine hoặc sensor
> tăng?

------------------------------------------------------------------------

# 40. Kết quả đầu ra kỳ vọng

Đồ án nên có các output sau:

## Machine Learning

``` text
model_random_forest.pkl
model_xgboost.json
evaluation.csv
```

## Streaming

``` text
Kafka topics
Spark streaming job
```

## Database

``` text
InfluxDB measurements
```

## Visualization

``` text
Grafana dashboard
```

## Documentation

``` text
Architecture
Deployment
Experiments
Results
```

------------------------------------------------------------------------

# 41. Tiêu chí đánh giá đồ án

Có thể chia:

  Nhóm                     Nội dung
  ------------------------ ------------------------------------
  Data Engineering         Dataset, cleaning, transformation
  Streaming                Kafka
  Distributed Processing   Spark
  Machine Learning         RF + XGBoost
  Time Series              Feature engineering + window
  Storage                  InfluxDB
  Visualization            Grafana
  Big Data Evaluation      Throughput + latency + scalability
  Research                 Experimental comparison
  Demo                     End-to-end system

------------------------------------------------------------------------

# 42. Hướng phát triển sau đồ án

Sau khi chứng minh bằng NASA C-MAPSS, có thể chuyển sang dữ liệu thật:

``` text
ESP32
 │
 ├── MPU6050 → Vibration
 ├── Temperature Sensor
 ├── Current Sensor
 ├── RPM Sensor
 └── Pressure Sensor
        │
        ▼
      MQTT
        │
        ▼
      Kafka
        │
        ▼
      Spark
        │
        ▼
    ML Model
        │
        ▼
 Predictive Maintenance
```

Có thể áp dụng cho:

``` text
Industrial Motor
Pump
Fan
Compressor
CNC
Robot
HVAC
Factory Equipment
```

Đây là hướng mở rộng từ **dataset-based research** sang **IoT +
Industrial AI thực tế**.

------------------------------------------------------------------------

# 43. Chiến lược triển khai khuyến nghị

Thứ tự nên là:

``` text
1. Understand dataset
        ↓
2. Create RUL
        ↓
3. Train baseline
        ↓
4. Feature Engineering
        ↓
5. Compare RF / XGBoost
        ↓
6. Kafka Producer
        ↓
7. Spark Streaming
        ↓
8. Online inference
        ↓
9. InfluxDB
        ↓
10. Grafana
        ↓
11. Performance benchmark
        ↓
12. Final demo
```

**Không nên làm ngược**, ví dụ dựng Kafka/Spark Cluster trước rồi mới
tìm hiểu RUL. Điều đó rất dễ khiến đồ án sa vào xử lý infrastructure.

------------------------------------------------------------------------

# 44. Kiến trúc cuối cùng

``` text
                         ┌───────────────────┐
                         │ NASA C-MAPSS      │
                         │ Historical Data   │
                         └─────────┬─────────┘
                                   │
                         ┌─────────▼─────────┐
                         │ Offline Training  │
                         │ RF / XGBoost      │
                         └─────────┬─────────┘
                                   │
                              ML Model
                                   │
                                   ▼
┌──────────────┐           ┌──────────────────┐
│ Sensor/Replay│ ────────► │      Kafka       │
└──────────────┘           └────────┬─────────┘
                                    │
                                    ▼
                           ┌──────────────────┐
                           │ Apache Spark     │
                           │ Structured       │
                           │ Streaming        │
                           └────────┬─────────┘
                                    │
                             Feature Engineering
                                    │
                                    ▼
                             ML Online Inference
                                    │
                                    ▼
                             RUL / Health Score
                                    │
                     ┌──────────────┴─────────────┐
                     ▼                            ▼
              ┌─────────────┐              ┌─────────────┐
              │ InfluxDB    │              │ Alert       │
              │ Time Series │              │ Engine      │
              └──────┬──────┘              └─────────────┘
                     │
                     ▼
              ┌─────────────┐
              │   Grafana   │
              │  Dashboard  │
              └─────────────┘
```

------------------------------------------------------------------------

# 45. Kết luận

Hệ thống cuối cùng cần chứng minh được 3 vấn đề:

### 1. Machine Learning

Có thể dự đoán:

``` text
Remaining Useful Life
```

từ dữ liệu cảm biến.

### 2. Big Data

Có thể:

``` text
Receive streaming data
        ↓
Distributed processing
        ↓
Near-real-time prediction
```

### 3. Industrial Monitoring

Có thể:

``` text
Sensor
 ↓
Prediction
 ↓
Health Status
 ↓
Visualization
 ↓
Maintenance Alert
```

Điểm cốt lõi của đồ án không phải chỉ là đạt một RMSE tốt, mà là xây
dựng được **một pipeline end-to-end có khả năng tiếp nhận, xử lý, dự
đoán và trực quan hóa dữ liệu cảm biến gần thời gian thực**.

------------------------------------------------------------------------

# 46. Tài liệu tham khảo chính

-   NASA C-MAPSS Jet Engine Simulated Data
-   Apache Kafka Documentation
-   Apache Spark Documentation
-   Spark MLlib Documentation
-   XGBoost Documentation
-   InfluxDB Documentation
-   Grafana Documentation

------------------------------------------------------------------------

# 47. Tiếng Anh

-   **Predictive Maintenance**: Bảo trì dự đoán
-   **Remaining Useful Life (RUL)**: Thời gian sống còn lại
-   **Degradation**: Sự suy giảm
-   **Failure**: Hỏng hóc
-   **Sensor Stream**: Luồng dữ liệu cảm biến
-   **Streaming Processing**: Xử lý dữ liệu luồng
-   **Distributed Processing**: Xử lý phân tán
-   **Feature Engineering**: Kỹ thuật tạo đặc trưng
-   **Sliding Window**: Cửa sổ trượt
-   **Throughput**: Thông lượng
-   **Latency**: Độ trễ
-   **Scalability**: Khả năng mở rộng
-   **Inference**: Suy luận mô hình
-   **Regression**: Hồi quy
-   **Health Score**: Điểm sức khỏe
-   **Maintenance Alert**: Cảnh báo bảo trì
-   **Time Series**: Chuỗi thời gian
-   **Data Pipeline**: Đường ống dữ liệu
-   **Near Real-Time**: Gần thời gian thực
-   **Data Leakage**: Rò rỉ dữ liệu
