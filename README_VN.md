<div align="center">

  <h1>Kien Truc Big Data Xu Ly Dong Thoi Gian Thuc Du Doan Thoi Gian Song Con (RUL) Va Bao Tri Du Doan Thiet Bi</h1>

  <h3>Xu Ly Phan Tan Luong Du Lieu · Trich Xuat Dac Trung Chuoi Thoi Gian Truc Tuyen · Phan Tich Tu Bien Den Dam May</h3>

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

  <a href="#tom-tat">Tom tat</a>
  |
  <a href="#kien-truc-he-thong">Kien truc he thong</a>
  |
  <a href="#co-so-ly-thuyet">Co so ly thuyet</a>
  |
  <a href="#danh-gia-thuc-nghiem">Danh gia thuc nghiem</a>
  |
  <a href="#huong-dan-trien-khai">Huong dan trien khai</a>
  |
  <a href="#cau-truc-du-an">Cau truc du an</a>
  |
  <a href="#tai-lieu-tham-khao">Tai lieu tham khao</a>
  |
  <a href="./README.md">English</a>

</div>

---

## Tom tat

Trong cac moi truong Internet van vat cong nghiep (Industrial IoT), cac chuoi cam bien da chieu duoc san sinh lien tuc voi dac tinh suy thoai phi tuyen tinh cung nhu muc do nhieu tin hieu phuc tap. Cac phuong phap bao tri truyen thong—bao tri sua chua khi xay ra hong hoc hoac bao tri phong ngua dinh ky—thuong phat sinh chi phi tai chinh lon, gay gian doan hoat dong ngoai du kien hoac thay the linh kien qua som khi van con kha nang su dung.

Du an nay de xuat mot kien truc Big Data phan tan, co kha nang chiu loi cao, duoc thiet ke de du doan Thoi gian song con lai (Remaining Useful Life - RUL) va danh gia trang thai suc khoe thiet bi theo thoi gian thuc. Su dung tap du lieu mo phong suy thoai dong co phan luc NASA C-MAPSS (Commercial Modular Aero-Propulsion System Simulation), he thong ket hop co che tiep nhan du lieu bang Apache Kafka voi cong cu xu ly phan tan Apache Spark Structured Streaming. Quy trinh tinh toan thuc hien cac phep bien doi dac trung dong hoc truot tren bo nho dem thoi gian thuc (gia tri trung binh truot, do lech chuan truot va xu huong bien thien) tren cac cua so thoi gian da quy mo, sau do ap dung mo hinh hoi quy Gradient Boosting (XGBoost) duoc vector hoa thong qua Apache Arrow. Ket qua du doan va phan loai trang thai duoc dua vao co so du lieu chuoi thoi gian InfluxDB va truc quan hoa tren bang dieu khien Grafana voi do tre thap.

---

## Kien truc he thong

Quy trinh xu ly dau-cuoi duoc tach biet thanh cac tang chuc nang doc lap nham dam bao tinh mo-dun va kha nang mo rong:

```
                  Du lieu Cam bien NASA C-MAPSS (FD001)
                                     |
                                     v
                       [Module Replay Phat Streaming]
                                     |
                                     | (Luong JSON phan vung, 10-5000 msg/s)
                                     v
                       [Apache Kafka Message Broker]
                         Topic: engine_sensor_raw
                                     |
                                     v
                   [Apache Spark Structured Streaming]
              +-----------------------------------------------+
              |  - Kiem dinh Schema va loai bo Sensor hang so |
              |  - Bo dem truot trang thai rieng tung Dong co |
              |  - Trich xuat dac trung chuoi thoi gian da cap|
              |  - Suy luan vector hoa bang XGBoost va Arrow  |
              |  - Tinh toan Health Score va phan cap canh bao|
              +-----------------------------------------------+
                                     |
                                     v
                    [Co so du lieu chuoi thoi gian InfluxDB v2]
                       Bucket: turbofan_telemetry
                                     |
                                     v
                     [Bang dieu khien giam sat Grafana]
        (Tong quan ham doi, Duong cong suy thoai RUL, Chi so cam bien)
```

---

## Tinh nang cot loi

<table align="center">
  <tr>
    <th><div align="center"> Xu ly phan tan dong du lieu </div></th>
    <th><div align="center"> Kho dac trung trang thai truc tuyen </div></th>
  </tr>
  <tr>
    <td>
      <div align="center">
        Dieu phoi streaming theo co che micro-batch qua Apache Spark Structured Streaming
        <br />
        Bao toan ngu nghia xu ly exactly-once nho co che luu checkpoint phan tan
      </div>
    </td>
    <td>
      <div align="center">
        Bo dem truot luu tru trang thai trong bo nho cho tung thiet bi rieng biet
        <br />
        Tinh toan tong hop da cua so ma khong gay ro ri du lieu giua cac thiet bi
      </div>
    </td>
  </tr>
  <tr>
    <td colspan="2"><!-- spacer row --></td>
  </tr>
  <tr>
    <th><div align="center"> Suy luan vector hoa toc do cao </div></th>
    <th><div align="center"> He thong luu tru chuoi thoi gian </div></th>
  </tr>
  <tr>
    <td>
      <div align="center">
        Loai bo chi phi tuan tu hoa IPC nho Apache Arrow va Pandas UDF vector hoa
        <br />
        Do tre suy luan dat 0.0007 ms tren moi ban ghi du lieu cam bien
      </div>
    </td>
    <td>
      <div align="center">
        Luu tru toi uu hoa va co che Write-Ahead Logging voi InfluxDB v2
        <br />
        Tu dong provisioning bang dieu khien Grafana de giam sat ham doi
      </div>
    </td>
  </tr>
</table>

---

## Co so ly thuyet

### 1. Mo hinh hoa Thoi gian song con lai (RUL)

Doi voi mot dong co $i$ hoat dong qua cac chu ky kiem tra roi rac $t \in [1, T_i]$, trong do $T_i$ la chu ky hong hoc cuoi cung, gia tri RUL tuyen tinh duoc xac dinh boi cong thuc:

$$RUL_{raw}(i, t) = T_i - t$$

Nham han che viec phat sai so qua lon trong giai doan dong co con moi (khi cac dau hieu suy thoai chua xuat hien ro ret), ham chan tren piecewise linear voi nguong $RUL_{max} = 125$ chu ky duoc ap dung:

$$RUL(i, t) = \min\left(RUL_{raw}(i, t), RUL_{max}\right)$$

### 2. Trich xuat dac trung chuoi thoi gian

Voi moi gia tri do $s_k(t)$ tu cam bien dong $k$ tai chu ky $t$, he thong thuc hien trich xuat dac trung da quy mo tren cac cua so quan sat qua khu $W \in \{10, 20\}$:

* **Gia tri trung binh truot (Rolling Mean):**

$$\mu_{k, W}(t) = \frac{1}{W} \sum_{\tau = 0}^{W-1} s_k(t - \tau)$$

* **Do lech chuan truot (Rolling Standard Deviation):**

$$\sigma_{k, W}(t) = \sqrt{\frac{1}{W-1} \sum_{\tau = 0}^{W-1} \left(s_k(t - \tau) - \mu_{k, W}(t)\right)^2}$$

* **Do bien thien truot / Xu huong (Rolling Delta / Trend):**

$$\Delta_{k, W}(t) = s_k(t) - s_k(t - W)$$

### 3. Tinh toan Diem suc khoe (Health Score) va Phan cap trang thai

Trang thai hoat dong cua thiet bi duoc luong hoa thanh chi so Health Score lien tuc $H(t) \in [0.0, 100.0]$:

$$H(t) = 100.0 \times \min\left(\frac{\widehat{RUL}(t)}{RUL_{ref}}, 1.0\right)$$

trong do $RUL_{ref} = 125.0$ chu ky. Cac phan cap canh bao duoc phan dinh theo cac nguong sau:

$$\text{Trang thai}(t) = \begin{cases} \text{HEALTHY}, & \text{neu } \widehat{RUL}(t) > 50 \\ \text{WARNING}, & \text{neu } 20 < \widehat{RUL}(t) \le 50 \\ \text{CRITICAL}, & \text{neu } \widehat{RUL}(t) \le 20 \end{cases}$$

### 4. Ham mat mat phi doi xung (NASA PHM 2008 Scoring Function)

Trong van hanh thuc te, viec du doan RUL som hon thuc te (bao tri phong ngua som) it gay nguy hiem hon rat nhieu so voi du doan muon hon thuc te (nguy co gay tai nan do dong co hong dot ngot). Do do, ham danh gia phi doi xung duoc ap dung:

$$d = \widehat{RUL} - RUL$$

$$S = \sum_{j=1}^{N} s_j, \quad s_j = \begin{cases} \exp\left(-\frac{d_j}{13}\right) - 1, & \text{khi } d_j < 0 \text{ (Du doan som)} \\ \exp\left(\frac{d_j}{10}\right) - 1, & \text{khi } d_j \ge 0 \text{ (Du doan muon)} \end{cases}$$

---

## Danh gia thuc nghiem

### So sanh hieu nang cac mo hinh hoc may

Thuc nghiem doi sanh duoc tien hanh tren tap kiem thu NASA C-MAPSS FD001 (100 dong co, 108 dac trung chuoi thoi gian):

| Chi so danh gia | Random Forest (Baseline) | XGBoost (San pham) | Nhan xet chuyen sau |
| :--- | :--- | :--- | :--- |
| Sai so tuyet doi trung binh (MAE) | 45.85 chu ky | 45.95 chu ky | Tuong duong (chenh lech < 0.2%) |
| Can bac hai sai so trung binh (RMSE) | 58.48 chu ky | 58.49 chu ky | Do phan tan sai so tuong dong |
| He so xac dinh (R²) | -0.8401 | -0.8406 | Do fit hoi quy co so |
| Diem phat phi doi xung NASA PHM | 90,163,286.5 | 94,836,017.5 | Tong diem phat tren 100 thiet bi |
| Thoi gian huan luyen (Wall-Clock) | 19.27 giay | 1.33 giay | XGBoost nhanh hon 14.5 lan |
| Do tre suy luan tren mot mau | 0.0023 ms | 0.0007 ms | XGBoost co do tre thap hon 3.3 lan |
| Dung luong file mo hinh sau dong goi | 65.0 MB | 1.3 MB | XGBoost nhe hon 50 lan, toi uu cho streaming |

### Kiem chung suy luan chu ky suy thoai thuc te

Ket qua kiem chung tren toan bo vong doi hoat dong cua dong co Engine #1 (tu chu ky 1 den chu ky hong 232):

```text
Giai doan van hanh    Chu ky          RUL du doan       Health Score     Trang thai he thong
Giai doan dau         Chu ky 1        125.00 chu ky     100.00%          HEALTHY (Xanh)
Giai doan dau         Chu ky 3        101.19 chu ky      80.95%          HEALTHY (Xanh)
Giai doan giua        Chu ky 121      116.39 chu ky      93.11%          HEALTHY (Xanh)
Giai doan suy thoai   Chu ky 200       34.12 chu ky      27.30%          WARNING (Vang)
Giai doan cuoi        Chu ky 228        5.26 chu ky       4.21%          CRITICAL (Do)
Giai doan cuoi        Chu ky 230        3.01 chu ky       2.41%          CRITICAL (Do)
Diem dung hoat dong   Chu ky 232        3.44 chu ky       2.75%          CRITICAL (Do)
```

Mo hinh du doan chinh xac su co hong hoc trong pham vi sai so 0.44 chu ky truoc khi dong co dung han, cung cap du thoi gian de he thong kich hoat canh bao bao tri.

---

## Huong dan trien khai

### 1. Yeu cau he thong

* He dieu hanh: Linux hoac macOS
* Python 3.12
* Docker va Docker Compose (phien ban 2.20 tro len)

### 2. Thiet lap moi truong lap trinh

Sao chep repository va khoi tao moi truong ao Python:

```bash
git clone https://github.com/Hdchipeo/predictive-maintenance.git
cd predictive-maintenance

python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 3. Huan luyen mo hinh va chay Benchmark

Thuc hien toan bo quy trinh chuan bi du lieu, loai bo sensor hang so, trich xuat dac trung va danh gia mo hinh:

```bash
python -m src.benchmark.benchmark_runner
```

File mo hinh sau khi huan luyen va ban mo ta metadata se duoc luu tai `models/xgboost_rul_v1.0/` va lien ket toi `models/latest/`.

### 4. Khoi dong cum ha tang Big Data bang Docker

Khoi chay cac container dich vu phan tan:

```bash
cd docker
docker-compose up -d
```

Cac cong ket noi dich vu:
* Bang dieu khien Grafana: `http://localhost:3000` (Tai khoan mac dinh: `admin` / `admin`)
* Giao dien InfluxDB: `http://localhost:8086` (To chuc: `predmaint_org`)
* Giao dien Spark Master: `http://localhost:8080`
* Cong ket noi Apache Kafka: `localhost:9092`

### 5. Khoi chay Pipeline xu ly Streaming

Mo 2 cua so terminal:

* **Terminal 1: Khoi dong Spark Structured Streaming Consumer**
```bash
source .venv/bin/activate
python -m src.streaming.stream_pipeline
```

* **Terminal 2: Khoi dong Replay Producer phat du lieu cam bien**
```bash
source .venv/bin/activate
python -m src.producer.replay_simulator --rate 50
```

### 6. Che do kiem thu cuc bo (Local Verification)

Doi voi cac may phat trien chua cai dat Docker, he thong cung cap che do kiem thu tich hop san:

```bash
python -m src.streaming.stream_pipeline --local-test
pytest tests/ -v
```

---

## Cau truc du an

```text
predictive-maintenance/
├── .gitignore                          # Cac file loai tru khoi quan ly phien ban
├── Makefile                            # Phim tat thuc thi lenh tu dong
├── README.md                           # Tai lieu ky thuat tieng Anh
├── README_VN.md                        # Tai lieu ky thuat tieng Viet
├── requirements.txt                    # Danh sach thu vien Python
├── docker/
│   ├── docker-compose.yml              # Cau hinh cum Kafka, InfluxDB, Grafana, Spark
│   └── grafana/
│       ├── provisioning/
│       │   ├── datasources/            # Cau hinh tu dong ket noi InfluxDB Flux
│       │   └── dashboards/             # Cau hinh provider cho dashboard
│       └── dashboards/
│           └── engine_health_dashboard.json # File JSON dashboard giam sat 6 panel
├── data/
│   ├── raw/                            # Du lieu NASA C-MAPSS (train, test, RUL)
│   └── processed/                      # Du lieu dac trung sau khi tien xu ly
├── models/
│   ├── latest/                         # Symbolic link tro toi mo hinh san pham
│   ├── rf_rul_v1.0/                    # Mo hinh Random Forest baseline va metadata
│   └── xgboost_rul_v1.0/               # Mo hinh XGBoost production va metadata
├── src/
│   ├── config.py                       # Tham so cau hinh toan cuc
│   ├── common/
│   │   ├── logger.py                   # Module ghi log co cau truc
│   │   └── metrics.py                  # Module do luong do tre va throughput
│   ├── preprocessing/
│   │   ├── loader.py                   # Parser du lieu C-MAPSS va bo tao du lieu thuc nghiem
│   │   ├── label_rul.py                # Tinh toan nhan RUL voi piecewise clipping
│   │   ├── clean.py                    # Loc bo cam bien phuong sai thap va lam sach du lieu
│   │   └── feature_engineering.py      # Tinh toan dac trung chuoi thoi gian da cua so
│   ├── training/
│   │   ├── train_rf.py                 # Module huan luyen Random Forest
│   │   ├── train_xgboost.py            # Module huan luyen XGBoost
│   │   ├── evaluate.py                 # Module danh gia (MAE, RMSE, NASA Score)
│   │   └── export.py                   # Module dong goi mo hinh va metadata
│   ├── producer/
│   │   ├── kafka_producer.py           # Kafka producer voi co che tu dong reconnect
│   │   └── replay_simulator.py         # Module mo phong stream du lieu da dong co
│   ├── streaming/
│   │   ├── spark_session.py            # Khoi tao SparkSession toi uu hoa PyArrow
│   │   ├── alert_engine.py             # Logic tinh Health Score va phan cap canh bao
│   │   ├── influx_sink.py              # Module ghi du lieu theo batch vao InfluxDB
│   │   ├── inference_engine.py         # Bo dem truot online va engine suy luan vector hoa
│   │   └── stream_pipeline.py          # Dinh nghia Spark Structured Streaming pipeline
│   └── benchmark/
│       └── benchmark_runner.py         # Runner do luong hieu nang tong the
├── notebooks/
│   ├── 01_eda_and_sensor_analysis.ipynb # Notebook phan tich kham pha du lieu (EDA)
│   └── 02_model_experimentation.ipynb  # Notebook thu nghiem va so sanh mo hinh
└── tests/
    ├── test_alert_engine.py            # Kiem thu Health Score va phan cap nguong
    ├── test_preprocessing.py           # Kiem thu tinh RUL va dac trung rolling
    └── test_producer_simulator.py      # Kiem thu simulator phat du lieu
```

---

## Kha nang chiu loi va Phuc hoi trang thai

1. **Bao toan State Store**: Spark Structured Streaming luu tru trang thai commit cua tung micro-batch tai thu muc `checkpoints/spark_stream/`. Khi mot node trong cum gap su co hoac he thong khoi dong lai, tien trinh se tiep tuc xu ly tu dung offset da duoc ghi nhan trong Kafka, dam bao tinh toan ven du lieu.
2. **Co che cach ly bo dem thiet bi**: Bo dem truot cua `src/streaming/inference_engine.py` duoc phan vung doc lap theo ma dinh danh `engine_id`, tranh hien tuong lan lon trang thai giua cac thiet bi khac nhau.
3. **Kha nang chiu loi ket noi**: Ca hai module `ResilientKafkaProducer` va `InfluxDBSink` deu duoc tich hop co che backoff-and-retry cung che do fallback du phong (`mock_mode`), giup tien trinh streaming khong bi dung khi he thong co so du lieu hoac message broker tam thoi mat ket noi.

---

## Tai lieu tham khao

1. **A. Saxena, K. Goebel, D. Simon, and N. Eklund**, "Damage Propagation Modeling for Aircraft Engine Run-to-Failure Simulation", in *Proceedings of the 1st International Conference on Prognostics and Health Management (PHM08)*, Denver, CO, Oct. 2008.
2. **NASA Prognostics Center of Excellence (PCoE)**, "Turbofan Engine Degradation Simulation Data Set", NASA Ames Research Center, Moffett Field, CA.
3. **T. Chen and C. Guestrin**, "XGBoost: A Scalable Tree Boosting System", in *Proceedings of the 22nd ACM SIGKDD International Conference on Knowledge Discovery and Data Mining*, pp. 785–794, 2016.
4. **M. Armbrust et al.**, "Structured Streaming: A Declarative Engine for Real-Time Applications on Apache Spark", in *Proceedings of the 2018 International Conference on Management of Data (SIGMOD)*, pp. 561–573, 2018.

---

## Trich dan

De trich dan repository nay trong cac cong bo hoc thuat:

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

## Giay phep

Du an duoc cap phep duoi Giay phep Apache 2.0. Xem chi tiet tai file [LICENSE](LICENSE).
