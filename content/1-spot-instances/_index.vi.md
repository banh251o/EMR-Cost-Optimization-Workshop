---
title: "Phần 1: Spot Instances"
weight: 10
chapter: false
---

# Phần 1: Sử dụng Spot Instances để giảm chi phí

Trong phần này, bạn sẽ học cách tạo EMR cluster sử dụng Spot Instances để giảm chi phí từ 60-70% so với On-Demand instances.

## Spot Instances là gì?

Spot Instances là các EC2 instances không sử dụng của AWS, được bán với giá thấp hơn nhiều so với On-Demand. Tuy nhiên, AWS có thể thu hồi chúng bất cứ lúc nào khi có nhu cầu.

**Ưu điểm:**
- Giá rẻ hơn 50-90% so với On-Demand
- Phù hợp cho workloads có thể chịu được interruption
- Tích hợp tốt với EMR

**Nhược điểm:**
- Có thể bị terminate bất cứ lúc nào
- Không phù hợp cho workloads critical
- Cần thiết kế application để handle interruption

## Chiến lược sử dụng Spot với EMR

### 1. Mixed Instance Types
Sử dụng nhiều loại instance khác nhau để tăng availability:
- m5.large, m4.large, c5.large
- Nếu một loại bị terminate, các loại khác vẫn chạy

### 2. Phân chia vai trò
- **Master node**: Luôn dùng On-Demand (quan trọng nhất)
- **Core nodes**: Một phần On-Demand, một phần Spot
- **Task nodes**: 100% Spot (có thể terminate mà không mất data)

### 3. Bidding Strategy
- Đặt bid price cao hơn current spot price 10-20%
- Theo dõi spot price history để đặt giá hợp lý

## Thực hành: Tạo EMR Cluster với Spot

### Bước 1: Chuẩn bị
Trước khi tạo cluster, bạn cần:
- Đăng nhập AWS Console
- Chọn region (khuyến nghị: us-east-1)
- Tạo EC2 Key Pair nếu chưa có
- Kiểm tra IAM roles: EMR_DefaultRole và EMR_EC2_DefaultRole

### Bước 2: Tạo Cluster qua Console
1. Mở AWS Console → EMR
2. Click "Create cluster"
3. Chọn "Go to advanced options"

**Cấu hình Software:**
- Release: emr-6.15.0
- Applications: Spark, Hadoop

**Cấu hình Hardware:**
- Master: 1 x m5.xlarge (On-Demand)
- Core: 1 x m5.large (On-Demand) + 2 x m5.large (Spot, bid $0.05)
- Task: 4 x m5.large (Spot, bid $0.05)

**Cấu hình General:**
- Cluster name: "Workshop-Spot-Cluster"
- Logging: Enable (chọn S3 bucket)
- Termination protection: Disable

### Bước 3: Theo dõi Cluster
Sau khi tạo cluster:
1. Theo dõi trạng thái trong EMR Console
2. Kiểm tra Hardware tab để xem instances
3. Xem Spot price history trong EC2 Console

### Bước 4: So sánh Chi phí

**Cấu hình On-Demand:**
- Master: 1 x m5.xlarge = $0.192/giờ
- Workers: 6 x m5.large = $0.576/giờ
- **Tổng: $0.768/giờ**

**Cấu hình Spot:**
- Master: 1 x m5.xlarge = $0.192/giờ
- Core On-Demand: 1 x m5.large = $0.096/giờ
- Spot instances: 6 x m5.large = $0.180/giờ (giả sử spot price $0.03)
- **Tổng: $0.468/giờ**

**Tiết kiệm: 39% ($0.30/giờ)**

## Xử lý Spot Interruption

### Monitoring Interruptions
EMR tự động xử lý spot interruptions:
- Task nodes bị terminate: Jobs được redistribute
- Core nodes bị terminate: Data được replicate
- Cluster tiếp tục chạy với instances còn lại

### Best Practices
1. **Checkpoint thường xuyên**: Lưu intermediate results
2. **Sử dụng S3**: Store data ngoài cluster
3. **Mixed instance types**: Giảm risk tất cả bị terminate cùng lúc
4. **Monitor spot prices**: Adjust bid prices khi cần

## Lab Exercise: Test Spot Interruption

### Tạo Test Job
1. SSH vào master node
2. Tạo file test job:

```python
# simple-job.py
from pyspark.sql import SparkSession
import time

spark = SparkSession.builder.appName("SpotTest").getOrCreate()

# Tạo large dataset
df = spark.range(0, 10000000).toDF("id")
df = df.repartition(100)

# Chạy job lâu để test interruption
for i in range(10):
    result = df.count()
    print(f"Iteration {i}: Count = {result}")
    time.sleep(60)  # Chờ 1 phút

spark.stop()

Submit job: spark-submit simple-job.py
Simulate Interruption
Trong EC2 Console, terminate một spot instance
Quan sát job vẫn tiếp tục chạy
Kiểm tra EMR Console xem cluster status
Kết quả Phần 1
Sau khi hoàn thành phần này, bạn đã:

✅ Hiểu cách Spot Instances hoạt động với EMR
✅ Tạo được cluster với mixed instance types
✅ Tiết kiệm 39% chi phí so với On-Demand
✅ Test được spot interruption handling
Chi phí thực tế: Cluster chạy 1 giờ = ~$0.47 (thay vì $0.77)

{{% notice success %}} Chúc mừng! Bạn đã tạo thành công EMR cluster với Spot Instances và tiết kiệm được chi phí đáng kể. Tiếp theo, chúng ta sẽ học Auto Scaling để tối ưu hóa thêm. {{% /notice %}}

{{% notice warning %}} Lưu ý: Đừng terminate cluster ngay! Chúng ta sẽ sử dụng cluster này cho Phần 2: Auto Scaling. {{% /notice %}}

Câu hỏi thường gặp
Q: Spot instances có đáng tin cậy không? A: Với EMR, spot instances rất đáng tin cậy vì EMR tự động handle interruptions. Chỉ cần thiết kế job properly.

**Q:Khi nào nên sử dụng Spot instances?**
A: Spot instances phù hợp cho batch processing, data analysis, machine learning training - các workloads có thể restart được.

**Q: Làm sao biết bid price phù hợp?**
A: Kiểm tra Spot Price History trong EC2 Console, đặt bid cao hơn average price 10-20%.

**Q: Nếu tất cả spot instances bị terminate thì sao?**
A: EMR sẽ tự động launch instances mới. Job có thể restart từ checkpoint gần nhất.

---

**Tiếp theo:** [Phần 2: Auto Scaling](/02-auto-scaling) để học cách tự động điều chỉnh cluster size theo workload.



