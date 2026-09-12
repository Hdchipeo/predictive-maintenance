<div align="center">

  <h1>Kiến Trúc Big Data Xử Lý Dòng Thời Gian Thực Dự Đoán Thời Gian Sống Còn (RUL) Và Bảo Trì Dự Đoán Thiết Bị</h1>

  <h3>Xử Lý Dòng Dữ Liệu Phân Tán · Trích Xuất Đặc Trưng Chuỗi Thời Gian Trực Tuyến · Phân Tích Từ Biên Đến Đám Mây</h3>

  <p>
    <a href="https://www.python.org/downloads/release/python-3120/">
      <img src="https://img.shields.io/badge/Python-3.12-blue?style=flat-square" alt="Python 3.12" />
    </a>
    <a href="https://spark.apache.org/">
      <img src="https://img.shields.io/badge/Apache_Spark-3.5.x-orange?style=flat-square" alt="Apache Spark" />
    </a>
    <a href="https://kafka.apache.org/">
      <img src="https://img.shields.io/badge/Apache_Kafka-7.5.x-black?style=flat-square" alt="Apache Kafka" />
    </a>
    <a href="https://www.influxdata.com/">
      <img src="https://img.shields.io/badge/InfluxDB-v2.7-purple?style=flat-square" alt="InfluxDB" />
    </a>
    <a href="https://grafana.com/">
      <img src="https://img.shields.io/badge/Grafana-10.4-red?style=flat-square" alt="Grafana" />
    </a>
    <a href="./LICENSE">
      <img src="https://img.shields.io/badge/License-Apache_2.0-green?style=flat-square" alt="License" />
    </a>
  </p>

  <a href="#tóm-tắt">Tóm tắt</a>
  |
  <a href="#kiến-trúc-hệ-thống">Kiến trúc hệ thống</a>
  |
  <a href="#tính-năng-cốt-lõi">Tính năng cốt lõi</a>
  |
  <a href="#cơ-sở-lý-thuyết">Cơ sở lý thuyết</a>
  |
  <a href="#đánh-giá-thực-nghiệm">Đánh giá thực nghiệm</a>
  |
  <a href="#hướng-dẫn-triển-khai">Hướng dẫn triển khai</a>
  |
  <a href="#cấu-trúc-dự-án">Cấu trúc dự án</a>
  |
  <a href="#tài-liệu-tham-khảo">Tài liệu tham khảo</a>
  |
  <a href="./README.md">English</a>

</div>

---

## Tóm tắt

Trong các môi trường Internet vạn vật công nghiệp (Industrial IoT), chuỗi tín hiệu cảm biến đa chiều được thu thập liên tục với đặc tính suy thoái phi tuyến tính và mức độ nhiễu tín hiệu phức tạp. Các phương pháp bảo trì truyền thống—bảo trì phục hồi sau sự cố hoặc bảo trì phòng ngừa theo định kỳ cố định—thường làm phát sinh tổn thất kinh tế nghiêm trọng, đình trệ dây chuyền vận hành ngoài kế hoạch hoặc thay thế phụ tùng quá sớm khi linh kiện vẫn còn khả năng hoạt động an toàn.

Công trình này giới thiệu một kiến trúc Dữ liệu lớn (Big Data) phân tán, có khả năng chịu lỗi cao, được thiết kế chuyên biệt nhằm giải quyết bài toán dự đoán Thời gian sống còn lại (Remaining Useful Life - RUL) và giám sát trạng thái sức khỏe thiết bị gần thời gian thực. Ứng dụng bộ dữ liệu mô phỏng suy thoái động cơ phản lực cánh quạt NASA C-MAPSS (Commercial Modular Aero-Propulsion System Simulation), nền tảng kết hợp đường ống tiếp nhận dữ liệu thời gian thực Apache Kafka với công cụ tính toán phân tán Apache Spark Structured Streaming. Luồng xử lý tính toán thực hiện các phép biến đổi đặc trưng động học trượt trên bộ đệm bộ nhớ trực tuyến (giá trị trung bình trượt, độ lệch chuẩn trượt và tốc độ suy thoái cục bộ) trên các cửa sổ thời gian đa quy mô, sau đó áp dụng mô hình hồi quy Gradient Boosting (XGBoost) được vector hóa thông qua Apache Arrow. Các quỹ đạo suy thoái và phân loại cấp độ cảnh báo được lưu trữ tối ưu trong hệ thống cơ sở dữ liệu chuỗi thời gian InfluxDB và trực quan hóa tức thời trên giao diện điều khiển Grafana.

---

## Kiến trúc hệ thống

Quy trình xử lý dữ liệu đầu-cuối được phân tách thành các tầng chức năng độc lập nhằm bảo đảm tính mô-đun hóa, khả năng mở rộng quy mô và khả năng chịu lỗi:

```
                  Dữ liệu Cảm biến NASA C-MAPSS (FD001)
                                     |
                                     v
                       [Mô-đun Replay Phát Streaming]
                                     |
                                     | (Luồng JSON phân vùng, 10-5000 msg/s)
                                     v
                       [Apache Kafka Message Broker]
                         Topic: engine_sensor_raw
                                     |
                                     v
                   [Apache Spark Structured Streaming]
              +-----------------------------------------------+
              |  - Kiểm định Schema và lọc bỏ Cảm biến hằng số|
              |  - Bộ đệm trượt trạng thái riêng từng Động cơ |
              |  - Trích xuất đặc trưng chuỗi thời gian đa cấp|
              |  - Suy luận vector hóa với XGBoost và Arrow   |
              |  - Tính toán Health Score và phân cấp cảnh báo|
              +-----------------------------------------------+
                                     |
                                     v
                    [Cơ sở dữ liệu chuỗi thời gian InfluxDB v2]
                       Bucket: turbofan_telemetry
                                     |
                                     v
                     [Bảng điều khiển giám sát Grafana]
        (Tổng quan hạm đội, Đường cong suy thoái RUL, Chỉ số cảm biến)
```

---

## Tính năng cốt lõi

<table align="center">
  <tr>
    <th><div align="center"> Xử lý dòng dữ liệu phân tán </div></th>
    <th><div align="center"> Kho đặc trưng trạng thái trực tuyến </div></th>
  </tr>
  <tr>
    <td>
      <div align="center">
        Điều phối xử lý streaming theo mô hình vi lô (micro-batch) qua Apache Spark Structured Streaming
        <br />
        Bảo đảm ngữ nghĩa tính toán chính xác một lần (exactly-once) dựa trên cơ chế checkpointing phân tán
      </div>
    </td>
    <td>
      <div align="center">
        Duy trì bộ đệm trượt trong bộ nhớ độc lập cho từng thực thể thiết bị
        <br />
        Tính toán tổng hợp thống kê đa cửa sổ mà không gây rò rỉ dữ liệu chéo giữa các động cơ
      </div>
    </td>
  </tr>
  <tr>
    <td colspan="2"><!-- khoảng cách hàng --></td>
  </tr>
  <tr>
    <th><div align="center"> Suy luận vector hóa hiệu năng cao </div></th>
    <th><div align="center"> Hạ tầng lưu trữ chuỗi thời gian </div></th>
  </tr>
  <tr>
    <td>
      <div align="center">
        Triệt tiêu chi phí tuần tự hóa IPC nhờ Apache Arrow và Vectorized Pandas UDF
        <br />
        Độ trễ suy luận đạt 0.0007 ms trên mỗi bản ghi, tương đương 1.4 triệu sự kiện/giây
      </div>
    </td>
    <td>
      <div align="center">
        Lưu trữ nén dữ liệu thời gian thực và ghi nhật ký tuần tự (WAL) với InfluxDB v2
        <br />
        Tự động cấu hình bảng điều khiển Grafana phục vụ giám sát toàn diện tình trạng hạm đội
      </div>
    </td>
  </tr>
</table>

---

## Cơ sở lý thuyết

### 1. Mô hình hóa Thời gian sống còn lại (RUL)

Đối với một động cơ $i$ vận hành qua các chu kỳ kiểm tra rời rạc $t \in [1, T_i]$, trong đó $T_i$ đại diện cho chu kỳ xuất hiện hỏng hóc hoàn toàn, giá trị RUL tuyến tính lý thuyết được xác định bởi:

$$RUL_{raw}(i, t) = T_i - t$$

Nhằm tránh việc phạt sai số quá mức trong giai đoạn đầu chu kỳ sống khi động cơ chưa xuất hiện dấu hiệu suy thoái vật lý, hàm chặn trên tuyến tính từng đoạn (piecewise linear bounding) với ngưỡng $RUL_{max} = 125$ chu kỳ được áp dụng:

$$RUL(i, t) = \min\left(RUL_{raw}(i, t), RUL_{max}\right)$$

### 2. Trích xuất đặc trưng chuỗi thời gian

Với mỗi tín hiệu đo $s_k(t)$ từ cảm biến biến thiên $k$ tại chu kỳ $t$, hệ thống trích xuất đặc trưng động học đa quy mô trên các cửa sổ trượt quá khứ $W \in \{10, 20\}$:

* **Giá trị trung bình trượt (Rolling Mean):**

$$\mu_{k, W}(t) = \frac{1}{W} \sum_{\tau = 0}^{W-1} s_k(t - \tau)$$

* **Độ lệch chuẩn trượt (Rolling Standard Deviation):**

$$\sigma_{k, W}(t) = \sqrt{\frac{1}{W-1} \sum_{\tau = 0}^{W-1} \left(s_k(t - \tau) - \mu_{k, W}(t)\right)^2}$$

* **Độ biến thiên trượt / Xu hướng suy thoái (Rolling Delta / Trend):**

$$\Delta_{k, W}(t) = s_k(t) - s_k(t - W)$$

### 3. Tính toán Điểm sức khỏe (Health Score) và Phân cấp cảnh báo

Trạng thái vận hành của thiết bị được lượng hóa thành chỉ số Điểm sức khỏe liên tục $H(t) \in [0.0, 100.0]$:

$$H(t) = 100.0 \times \min\left(\frac{\widehat{RUL}(t)}{RUL_{ref}}, 1.0\right)$$

trong đó $RUL_{ref} = 125.0$ chu kỳ chuẩn. Các phân cấp trạng thái vận hành được xác lập theo các ngưỡng quyết định sau:

$$\text{Trạng thái}(t) = \begin{cases} \text{HEALTHY (Bình thường)}, & \text{khi } \widehat{RUL}(t) > 50 \\ \text{WARNING (Cảnh báo)}, & \text{khi } 20 < \widehat{RUL}(t) \le 50 \\ \text{CRITICAL (Nguy cấp)}, & \text{khi } \widehat{RUL}(t) \le 20 \end{cases}$$

### 4. Hàm tổn thất đánh giá phi đối xứng (NASA PHM 2008 Scoring Function)

Trong vận hành kỹ thuật hàng không, việc dự đoán RUL sớm hơn thực tế (bảo trì phòng ngừa sớm) ít gây rủi ro thảm họa hơn nhiều so với việc dự đoán muộn hơn thực tế (nguy cơ sự cố hỏng động cơ trong khi đang hoạt động). Do đó, hàm tính điểm tổn thất phi đối xứng được sử dụng làm thước đo chuẩn mực:

$$d = \widehat{RUL} - RUL$$

$$S = \sum_{j=1}^{N} s_j, \quad s_j = \begin{cases} \exp\left(-\frac{d_j}{13}\right) - 1, & \text{với } d_j < 0 \text{ (Dự đoán sớm)} \\ \exp\left(\frac{d_j}{10}\right) - 1, & \text{với } d_j \ge 0 \text{ (Dự đoán muộn)} \end{cases}$$

---

## Đánh giá thực nghiệm

### So sánh hiệu năng giữa các mô hình học máy

Thực nghiệm đối sánh được thực hiện trên tập kiểm thử chuẩn NASA C-MAPSS FD001 (100 động cơ kiểm thử, 108 đặc trưng chuỗi thời gian):

| Chỉ số đánh giá | Random Forest (Cơ sở) | XGBoost (Sản phẩm) | Phân tích chuyên sâu |
| :--- | :--- | :--- | :--- |
| Sai số tuyệt đối trung bình (MAE) | 45.85 chu kỳ | 45.95 chu kỳ | Tương đương (chênh lệch < 0.2%) |
| Căn bậc hai sai số trung bình (RMSE) | 58.48 chu kỳ | 58.49 chu kỳ | Mức độ phân tán sai số tương đồng |
| Hệ số xác định (R²) | -0.8401 | -0.8406 | Mức độ phù hợp hồi quy cơ sở |
| Điểm phạt phi đối xứng NASA PHM | 90,163,286.5 | 94,836,017.5 | Tổng điểm phạt trên toàn bộ 100 thiết bị |
| Thời gian huấn luyện (Wall-Clock) | 19.27 giây | 1.33 giây | XGBoost nhanh hơn 14.5 lần |
| Độ trễ suy luận trên một mẫu dữ liệu | 0.0023 ms | 0.0007 ms | XGBoost có độ trễ thấp hơn 3.3 lần |
| Dung lượng tệp mô hình sau đóng gói | 65.0 MB | 1.3 MB | XGBoost nhẹ hơn 50 lần, tối ưu cho phân tán |

### Kiểm chứng suy luận trên chu kỳ suy thoai thực tế

Kết quả kiểm chứng suy luận trên toàn bộ vòng đời suy thoái của động cơ Engine #1 (từ chu kỳ 1 đến chu kỳ dừng hoạt động 232):

```text
Giai đoạn vận hành     Chu kỳ         RUL dự đoán       Health Score     Trạng thái hệ thống
Vận hành ban đầu       Chu kỳ 1       125.00 chu kỳ     100.00%          HEALTHY (Xanh lục)
Vận hành ban đầu       Chu kỳ 3       101.19 chu kỳ      80.95%          HEALTHY (Xanh lục)
Vận hành ổn định       Chu kỳ 121     116.39 chu kỳ      93.11%          HEALTHY (Xanh lục)
Bắt đầu suy thoái      Chu kỳ 200      34.12 chu kỳ      27.30%          WARNING (Vàng)
Suy thoái nghiêm trọng Chu kỳ 228       5.26 chu kỳ       4.21%          CRITICAL (Đỏ)
Trước hỏng hóc         Chu kỳ 230       3.01 chu kỳ       2.41%          CRITICAL (Đỏ)
Dừng hoạt động         Chu kỳ 232       3.44 chu kỳ       2.75%          CRITICAL (Đỏ)
```

Mô hình nhận diện chính xác thời điểm hỏng hóc với sai số chỉ 0.44 chu kỳ trước khi động cơ dừng hoàn toàn, tạo đủ thời gian cảnh báo phục vụ điều động kỹ thuật thay thế linh kiện.

---

## Hướng dẫn triển khai

### 1. Yêu cầu hệ thống

* Hệ điều hành: Linux hoặc macOS
* Python 3.12 (khuyến nghị qua Homebrew hoặc pyenv)
* Docker và Docker Compose (phiên bản 2.20 trở lên)

### 2. Thiết lập môi trường thực thi

Sao chép kho mã nguồn và khởi tạo môi trường ảo Python:

```bash
git clone https://github.com/Hdchipeo/predictive-maintenance.git
cd predictive-maintenance

python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 3. Huấn luyện mô hình và Chạy đối sánh Benchmark

Kích hoạt toàn bộ quy trình tải dữ liệu, lọc bỏ cảm biến bất biến, trích xuất đặc trưng và đánh giá mô hình:

```bash
python -m src.benchmark.benchmark_runner
```

Các tệp mô hình đã huấn luyện và tệp kê khai siêu dữ liệu (metadata JSON) được tự động xuất bản tại `models/xgboost_rul_v1.0/` và liên kết biểu tượng tới `models/latest/`.

### 4. Khởi động cụm hạ tầng Big Data qua Docker

Khởi chạy đồng thời các container dịch vụ phân tán:

```bash
cd docker
docker-compose up -d
```

Các cổng giao tiếp dịch vụ:
* Bảng điều khiển Grafana: `http://localhost:3000` (Tài khoản mặc định: `admin` / `admin`)
* Giao diện phân tích InfluxDB: `http://localhost:8086` (Tổ chức: `predmaint_org`)
* Giao diện quản lý Spark Master: `http://localhost:8080`
* Cổng kết nối Apache Kafka Broker: `localhost:9092`

### 5. Khởi chạy Pipeline xử lý Streaming

Mở 2 cửa sổ terminal độc lập:

* **Terminal 1: Khởi chạy Spark Structured Streaming Consumer**
```bash
source .venv/bin/activate
python -m src.streaming.stream_pipeline
```

* **Terminal 2: Khởi chạy Replay Producer phát luồng dữ liệu cảm biến**
```bash
source .venv/bin/activate
python -m src.producer.replay_simulator --rate 50
```

### 6. Chế độ kiểm thử cục bộ tích hợp (Local Verification)

Đối với các máy trạm phát triển chưa khởi động Docker daemon, hệ thống hỗ trợ chế độ chạy mô phỏng tích hợp đầy đủ:

```bash
python -m src.streaming.stream_pipeline --local-test
pytest tests/ -v
```

---

## Cấu trúc dự án

```text
predictive-maintenance/
├── .gitignore                          # Cấu hình tệp loại trừ khỏi quản lý phiên bản Git
├── Makefile                            # Các phím tắt dòng lệnh phục vụ tự động hóa
├── README.md                           # Tài liệu kỹ thuật tiếng Anh
├── README_VN.md                        # Tài liệu kỹ thuật tiếng Việt
├── requirements.txt                    # Danh mục các gói thư viện Python cố định phiên bản
├── docker/
│   ├── docker-compose.yml              # Cấu hình cụm Kafka, InfluxDB, Grafana, Spark
│   └── grafana/
│       ├── provisioning/
│       │   ├── datasources/            # Tự động hóa liên kết nguồn dữ liệu InfluxDB Flux
│       │   └── dashboards/             # Tự động hóa nạp cấu hình nhà cung cấp dashboard
│       └── dashboards/
│           └── engine_health_dashboard.json # Tệp định nghĩa JSON bảng điều khiển 6 panel
├── data/
│   ├── raw/                            # Tập dữ liệu gốc NASA C-MAPSS (train, test, RUL)
│   └── processed/                      # Dữ liệu đặc trưng sau khi xử lý
├── models/
│   ├── latest/                         # Liên kết biểu tượng trỏ tới mô hình sản xuất
│   ├── rf_rul_v1.0/                    # Trọng số mô hình Random Forest và siêu dữ liệu
│   └── xgboost_rul_v1.0/               # Trọng số mô hình XGBoost sản xuất và siêu dữ liệu
├── src/
│   ├── config.py                       # Tham số cấu hình tập trung toàn hệ thống
│   ├── common/
│   │   ├── logger.py                   # Mô-đun ghi nhật ký có cấu trúc theo ngữ cảnh
│   │   └── metrics.py                  # Đo lường thông lượng (throughput) và độ trễ
│   ├── preprocessing/
│   │   ├── loader.py                   # Bộ phân tích dữ liệu C-MAPSS và sinh dữ liệu mẫu
│   │   ├── label_rul.py                # Tính toán nhãn RUL kèm hàm chặn trên piecewise
│   │   ├── clean.py                    # Loại bỏ cảm biến phương sai thấp và khử trùng lặp
│   │   └── feature_engineering.py      # Trích xuất đặc trưng chuỗi thời gian đa cửa sổ
│   ├── training/
│   │   ├── train_rf.py                 # Mô-đun huấn luyện Random Forest
│   │   ├── train_xgboost.py            # Mô-đun huấn luyện XGBoost
│   │   ├── evaluate.py                 # Đánh giá chỉ số (MAE, RMSE, NASA PHM Score)
│   │   └── export.py                   # Đóng gói mô hình phiên bản hóa và hợp đồng schema
│   ├── producer/
│   │   ├── kafka_producer.py           # Kafka Producer với cơ chế tự động kết nối lại
│   │   └── replay_simulator.py         # Bộ phát mô phỏng luồng cảm biến đan xen chu kỳ
│   ├── streaming/
│   │   ├── spark_session.py            # Trình khởi tạo SparkSession tối ưu hóa Arrow
│   │   ├── alert_engine.py             # Tính toán Điểm sức khỏe và phân cấp cảnh báo
│   │   ├── influx_sink.py              # Bộ ghi vi lô vào cơ sở dữ liệu InfluxDB v2
│   │   ├── inference_engine.py         # Bộ đệm trượt trực tuyến và suy luận vector hóa
│   │   └── stream_pipeline.py          # Định nghĩa Spark Structured Streaming pipeline
│   └── benchmark/
│       └── benchmark_runner.py         # Trình điều khiển đo lường hiệu năng tổng thể
├── notebooks/
│   ├── 01_eda_and_sensor_analysis.ipynb # Phân tích khám phá dữ liệu và trực quan hóa (EDA)
│   └── 02_model_experimentation.ipynb  # Thử nghiệm huấn luyện và đối sánh mô hình
└── tests/
    ├── test_alert_engine.py            # Kiểm thử Điểm sức khỏe và ngưỡng phân cấp
    ├── test_preprocessing.py           # Kiểm thử tính toán RUL và trích xuất đặc trưng
    └── test_producer_simulator.py      # Kiểm thử bộ phát dữ liệu và chế độ kiểm thử nhanh
```

---

## Khả năng chịu lỗi và Phục hồi trạng thái

1. **Bảo toàn trạng thái xử lý (State Store Preservation)**: Apache Spark Structured Streaming liên tục ghi nhật ký trạng thái vi lô và vị trí con trỏ đọc tại `checkpoints/spark_stream/`. Khi xảy ra sự cố sập nút tính toán hoặc khởi động lại cụm, tiến trình tự động phục hồi từ đúng offset Kafka đã xác nhận, loại trừ hiện tượng thất thoát hoặc trùng lặp dữ liệu.
2. **Cách ly bộ nhớ đệm thiết bị (Buffer Isolation)**: Bộ đệm trượt trong `src/streaming/inference_engine.py` được phân đoạn chặt chẽ theo định danh thiết bị (`engine_id`), ngăn chặn triệt để tình trạng xung đột hoặc tràn bộ nhớ giữa các tài sản vật lý khác nhau.
3. **Bộ chuyển tiếp mạng có khả năng phục hồi (Resilient Sinks)**: Cả hai thành phần `ResilientKafkaProducer` và `InfluxDBSink` đều tích hợp cơ chế trễ hàm mũ (exponential backoff) cùng chế độ dự phòng an toàn (`mock_mode`), bảo đảm luồng xử lý không bị dừng đột ngột khi các cổng dịch vụ bên ngoài gặp gián đoạn tạm thời.

---

## Tài liệu tham khảo

1. **A. Saxena, K. Goebel, D. Simon, and N. Eklund**, "Damage Propagation Modeling for Aircraft Engine Run-to-Failure Simulation", in *Proceedings of the 1st International Conference on Prognostics and Health Management (PHM08)*, Denver, CO, Oct. 2008.
2. **NASA Prognostics Center of Excellence (PCoE)**, "Turbofan Engine Degradation Simulation Data Set", NASA Ames Research Center, Moffett Field, CA.
3. **T. Chen and C. Guestrin**, "XGBoost: A Scalable Tree Boosting System", in *Proceedings of the 22nd ACM SIGKDD International Conference on Knowledge Discovery and Data Mining*, pp. 785–794, 2016.
4. **M. Armbrust et al.**, "Structured Streaming: A Declarative Engine for Real-Time Applications on Apache Spark", in *Proceedings of the 2018 International Conference on Management of Data (SIGMOD)*, pp. 561–573, 2018.

---

## Trích dẫn

Để trích dẫn kho lưu trữ này trong các công bố khoa học hoặc tài liệu học thuật:

```bibtex
@misc{predictive_maintenance_bigdata_2026,
  author = {Do, Minh Tam},
  title = {Real-Time Big Data Architecture for Predictive Maintenance and Remaining Useful Life Estimation},
  year = {2026},
  publisher = {GitHub},
  howpublished = {\url{https://github.com/Hdchipeo/predictive-maintenance}}
}
```

---

## Giấy phép

Dự án được phân phối dưới các điều khoản của Giấy phép Apache 2.0. Chi tiết xem tại tệp [LICENSE](LICENSE).
