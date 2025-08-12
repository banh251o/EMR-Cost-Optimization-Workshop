---
title: "Phần 2: Auto Scaling"
weight: 20
chapter: false
---

# Phần 2: EMR Auto Scaling - Tự động điều chỉnh theo workload

Trong phần này, bạn sẽ học cách thiết lập EMR Auto Scaling để cluster tự động tăng giảm số lượng instances theo workload, giúp tối ưu hóa cả chi phí và performance.

## EMR Auto Scaling là gì?

EMR Auto Scaling tự động điều chỉnh số lượng instances trong cluster dựa trên:
- **YARN metrics**: Memory và CPU utilization
- **Custom metrics**: CloudWatch metrics tùy chỉnh
- **Time-based scaling**: Theo lịch trình định sẵn

**Lợi ích:**
- Tiết kiệm chi phí khi workload thấp
- Tăng performance khi workload cao
- Không cần can thiệp thủ công
- Tích hợp tốt với Spot Instances

## Các loại Auto Scaling

### 1. EMR Managed Scaling (Khuyến nghị)
- AWS quản lý hoàn toàn
- Dựa trên YARN container pending
- Đơn giản, hiệu quả
- Hỗ trợ cả On-Demand và Spot

### 2. Custom Auto Scaling
- Tự định nghĩa scaling policies
- Dựa trên CloudWatch metrics
- Linh hoạt hơn nhưng phức tạp
- Phù hợp cho use cases đặc biệt

## Thực hành: Thiết lập Managed Scaling

### Bước 1: Enable Managed Scaling
Sử dụng cluster từ Phần 1, chúng ta sẽ enable auto scaling:

1. Mở EMR Console
2. Chọn cluster "Workshop-Spot-Cluster"
3. Vào tab "Configuration"
4. Click "Edit" ở phần "Scaling"

**Cấu hình Managed Scaling:**
- Minimum capacity: 2 instances
- Maximum capacity: 10 instances
- Maximum On-Demand capacity: 4 instances

### Bước 2: Cấu hình Advanced Settings
**Scale-out settings:**
- Scale out cooldown: 300 seconds
- Maximum scale-out increment: 100%

**Scale-in settings:**
- Scale in cooldown: 300 seconds
- Maximum scale-in increment: 50%

### Bước 3: Kiểm tra Configuration
Sau khi enable, kiểm tra:
- Scaling status: "Enabled"
- Current capacity: 7 instances (từ Phần 1)
- Target capacity: Sẽ thay đổi theo workload

## Test Auto Scaling

### Tạo Workload để Test Scaling

1. **SSH vào Master Node:**
`ssh -i your-key.pem hadoop@master-public-ip`

2. **Tạo Test Script:**
``python
# scaling-test.py
from pyspark.sql import SparkSession
import time

spark = SparkSession.builder \
    .appName("AutoScalingTest") \
    .config("spark.sql.adaptive.enabled", "false") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "false") \
    .getOrCreate()

print("=== Starting Auto Scaling Test ===")

# Tạo large dataset để trigger scaling
print("Creating large dataset...")
df = spark.range(0, 100000000).toDF("id")
df = df.repartition(200)  # Nhiều partitions để cần nhiều resources

# Cache để consume memory
df.cache()

# Chạy multiple operations để maintain load
for i in range(5):
    print(f"Running operation {i+1}/5...")
    
    # Heavy computation
    result = df.filter(df.id % 2 == 0).count()
    print(f"Even numbers count: {result}")
    
    # Giữ load trong 5 phút để observe scaling
    time.sleep(300)

print("=== Test completed ===")
spark.stop()
``

3. **Submit Job:**
`spark-submit \
    --executor-memory 2g \
    --num-executors 15 \
    --executor-cores 2 \
    scaling-test.py`

### Theo dõi Scaling Process

**Trong EMR Console:**
1. Vào tab "Hardware" để xem instances
2. Refresh mỗi 2-3 phút
3. Quan sát số lượng instances tăng lên

**Trong CloudWatch:**
1. Mở CloudWatch Console
2. Vào "Metrics" → "AWS/ElasticMapReduce"
3. Chọn metrics:
   - YARNMemoryAvailablePercentage
   - ContainerPending
   - AppsRunning

**Expected Behavior:**
- **Phút 0-2**: Job bắt đầu, YARN memory giảm
- **Phút 2-5**: ContainerPending tăng, scaling triggered
- **Phút 5-8**: Instances mới được add (2-3 instances)
- **Phút 8-25**: Job chạy với capacity mới
- **Phút 25-30**: Job kết thúc, scaling down bắt đầu

## Monitoring và Troubleshooting

### Key Metrics để Monitor

**YARN Metrics:**
- **YARNMemoryAvailablePercentage**: < 15% trigger scale out
- **ContainerPending**: > 0 trong 5 phút trigger scale out
- **AppsRunning**: Số applications đang chạy

**EMR Metrics:**
- **RunningMapTasks**: Map tasks đang chạy
- **RunningReduceTasks**: Reduce tasks đang chạy
- **TotalLoad**: Tổng load của cluster

### Common Issues và Solutions

**Issue 1: Scaling không hoạt động**
- Kiểm tra IAM permissions
- Verify scaling limits (min/max capacity)
- Check cooldown periods

**Issue 2: Scale out quá chậm**
- Giảm scale-out cooldown
- Tăng maximum scale-out increment
- Sử dụng multiple instance types

**Issue 3: Scale in quá nhanh**
- Tăng scale-in cooldown
- Giảm maximum scale-in increment
- Adjust YARN memory thresholds

## Advanced Scaling Strategies

### 1. Mixed Instance Types cho Scaling
Cấu hình multiple instance types để tăng availability:

**Instance Fleet Configuration:**
- Primary: m5.large (Spot)
- Secondary: m4.large (Spot)
- Fallback: c5.large (Spot)
- Emergency: m5.large (On-Demand)

### 2. Time-based Scaling
Cho workloads có pattern cố định:
- Scale out trước peak hours
- Scale in sau off-peak hours
- Sử dụng CloudWatch Events + Lambda

### 3. Predictive Scaling
Dựa trên historical data:
- Analyze past workload patterns
- Pre-scale cho expected load
- Combine với reactive scaling

## Cost Optimization với Auto Scaling

### Before Auto Scaling (Static Cluster)
- **Peak capacity**: 10 instances × 8 hours = 80 instance-hours
- **Cost**: 80 × $0.096 = $7.68/day

### After Auto Scaling (Dynamic Cluster)
- **Average capacity**: 4 instances × 8 hours = 32 instance-hours
- **Peak capacity**: 8 instances × 2 hours = 16 instance-hours
- **Total**: 32 + 16 = 48 instance-hours
- **Cost**: 48 × $0.096 = $4.61/day

**Tiết kiệm**: $3.07/day (40% cost reduction)

## Lab Exercise: Custom Scaling Policy

### Tạo Custom CloudWatch Alarm

1. **Tạo Scale-Out Alarm:**
`aws cloudwatch put-metric-alarm \
    --alarm-name "EMR-ScaleOut-HighMemory" \
    --alarm-description "Scale out when memory usage > 80%" \
    --metric-name YARNMemoryAvailablePercentage \
    --namespace AWS/ElasticMapReduce \
    --statistic Average \
    --period 300 \
    --threshold 20 \
    --comparison-operator LessThanThreshold \
    --evaluation-periods 2 \
    --dimensions Name=JobFlowId,Value=j-xxxxx`

2. **Tạo Scale-In Alarm:**
`aws cloudwatch put-metric-alarm \
    --alarm-name "EMR-ScaleIn-LowMemory" \
    --alarm-description "Scale in when memory usage < 30%" \
    --metric-name YARNMemoryAvailablePercentage \
    --namespace AWS/ElasticMapReduce \
    --statistic Average \
    --period 600 \
    --threshold 70 \
    --comparison-operator GreaterThanThreshold \
    --evaluation-periods 3 \
    --dimensions Name=JobFlowId,Value=j-xxxxx`

### Test Custom Scaling

1. **Tạo Light Workload:**
``python
# light-workload.py
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("LightWorkload").getOrCreate()

# Small dataset
df = spark.range(0, 1000000).toDF("id")
result = df.count()
print(f"Light workload result: {result}")

spark.stop()
``

2. **Submit và Monitor:**
`spark-submit light-workload.py`

3. **Quan sát Scale-In:**
- Memory usage giảm xuống < 30%
- Sau 10 phút, cluster scale in
- Instances giảm từ 8 xuống 4

## Production Best Practices

### 1. Scaling Configuration
**Recommended Settings:**
- Min capacity: 20% của peak capacity
- Max capacity: 150% của expected peak
- Scale-out cooldown: 300 seconds
- Scale-in cooldown: 600 seconds

### 2. Instance Mix Strategy
**Optimal Mix:**
- 30% On-Demand (stability)
- 70% Spot (cost savings)
- Multiple instance families
- Diversified AZs

### 3. Application Design
**Scaling-Friendly Applications:**
- Stateless processing
- Checkpointing enabled
- Graceful handling của node loss
- Partition data appropriately

### 4. Monitoring Setup
**Essential Metrics:**
- Cluster utilization
- Scaling events
- Job completion times
- Cost per job

## Kết quả Phần 2

Sau khi hoàn thành phần này, bạn đã:
- ✅ Thiết lập EMR Managed Scaling thành công
- ✅ Test scaling với real workload
- ✅ Hiểu cách monitor scaling metrics
- ✅ Tối ưu hóa thêm 40% chi phí với dynamic scaling

**Tổng tiết kiệm đến giờ:**
- Spot Instances: 39% (từ Phần 1)
- Auto Scaling: 40% (từ Phần 2)
- **Combined savings**: ~65% so với static On-Demand cluster

{{% notice success %}}
**Xuất sắc!** Cluster của bạn giờ đã tự động scale theo workload và tiết kiệm tối đa chi phí. Tiếp theo, chúng ta sẽ thiết lập monitoring để theo dõi mọi thứ.
{{% /notice %}}

## Troubleshooting Common Issues

### Issue: Scaling Events không xuất hiện
**Nguyên nhân có thể:**
- IAM role thiếu permissions
- Cooldown period chưa hết
- Metrics chưa đạt threshold

**Giải pháp:**
1. Kiểm tra CloudTrail logs
2. Verify EMR service role permissions
3. Adjust threshold values

### Issue: Scale-in quá aggressive
**Triệu chứng:**
- Instances bị terminate khi job vẫn chạy
- Performance degradation

**Giải pháp:**
1. Tăng scale-in cooldown lên 900s
2. Giảm maximum scale-in increment xuống 25%
3. Set higher memory threshold (80% thay vì 70%)

### Issue: Spot instances không được add khi scaling
**Nguyên nhân:**
- Spot capacity không available
- Bid price quá thấp
- Instance type constraints

**Giải pháp:**
1. Thêm multiple instance types
2. Tăng bid price
3. Enable multiple AZs

## Performance Tuning Tips

### 1. Optimize Spark Configuration
`# Spark configs cho auto-scaling environment
spark.dynamicAllocation.enabled=true
spark.dynamicAllocation.minExecutors=2
spark.dynamicAllocation.maxExecutors=50
spark.dynamicAllocation.initialExecutors=5`

### 2. YARN Configuration
``xml
<!-- yarn-site.xml optimizations -->
<property>
    <name>yarn.scheduler.capacity.resource-calculator</name>
    <value>org.apache.hadoop.yarn.util.resource.DominantResourceCalculator</value>
</property>
``

### 3. EMR Steps Optimization
- Sử dụng cluster mode thay vì client mode
- Enable speculation cho fault tolerance
- Configure appropriate parallelism

## Real-world Example: E-commerce Analytics

### Scenario
Công ty e-commerce cần process log data hàng ngày:
- **Morning (6-10 AM)**: Light processing (2-3 instances)
- **Afternoon (2-6 PM)**: Heavy analytics (8-12 instances)
- **Night (10 PM-2 AM)**: Batch reports (4-6 instances)

### Auto Scaling Configuration
`# Managed scaling policy
{
    "ComputeLimits": {
        "UnitType": "Instances",
        "MinimumCapacityUnits": 2,
        "MaximumCapacityUnits": 15,
        "MaximumOnDemandCapacityUnits": 5,
        "MaximumCoreCapacityUnits": 8
    }
}`

### Cost Comparison
**Before Auto Scaling:**
- Static 12 instances × 24 hours = 288 instance-hours
- Cost: 288 × $0.096 = $27.65/day

**After Auto Scaling:**
- Average 5 instances × 24 hours = 120 instance-hours
- Cost: 120 × $0.096 = $11.52/day
- **Savings: $16.13/day (58%)**

## Advanced Monitoring Setup

### Custom Metrics Dashboard
Tạo CloudWatch dashboard với:
- Cluster capacity over time
- Cost per hour tracking
- Job completion rates
- Scaling events timeline

### Automated Alerts
Setup alerts cho:
- Scaling failures
- High cost thresholds
- Performance degradation
- Spot interruption rates

### Cost Tracking Script
``python
# cost-tracker.py
import boto3
from datetime import datetime, timedelta

def track_emr_costs(cluster_id):
    emr = boto3.client('emr')
    ce = boto3.client('ce')
    
    # Get cluster info
    cluster = emr.describe_cluster(ClusterId=cluster_id)
    start_time = cluster['Cluster']['Status']['Timeline']['CreationDateTime']
    
    # Calculate cost
    end_time = datetime.now()
    
    response = ce.get_cost_and_usage(
        TimePeriod={
            'Start': start_time.strftime('%Y-%m-%d'),
            'End': end_time.strftime('%Y-%m-%d')
        },
        Granularity='DAILY',
        Metrics=['BlendedCost'],
        GroupBy=[
            {'Type': 'DIMENSION', 'Key': 'SERVICE'}
        ]
    )
    
    print(f"Cluster {cluster_id} cost tracking:")
    for result in response['ResultsByTime']:
        for group in result['Groups']:
            if 'ElasticMapReduce' in group['Keys'][0]:
                cost = group['Metrics']['BlendedCost']['Amount']
                print(f"Date: {result['TimePeriod']['Start']}, Cost: ${cost}")

# Usage
track_emr_costs('j-xxxxx')
``

## Scaling Patterns Analysis

### Pattern 1: Batch Processing
**Characteristics:**
- Predictable workload times
- High resource usage during processing
- Idle periods between jobs

**Optimal Strategy:**
- Aggressive scale-out (200% increment)
- Conservative scale-in (25% increment)
- Longer cooldown periods (600s)

### Pattern 2: Interactive Analytics
**Characteristics:**
- Unpredictable query patterns
- Variable resource requirements
- Need for quick response times

**Optimal Strategy:**
- Moderate scale-out (100% increment)
- Quick scale-in (50% increment)
- Shorter cooldown periods (300s)

### Pattern 3: Streaming Workloads
**Characteristics:**
- Continuous data processing
- Steady resource usage
- Occasional spikes

**Optimal Strategy:**
- Conservative scaling (50% increment)
- Maintain minimum baseline
- Focus on stability over cost

## Integration với Other AWS Services

### 1. Lambda Triggers
Tự động start/stop clusters:
``python
# lambda-emr-scheduler.py
import boto3
import json

def lambda_handler(event, context):
    emr = boto3.client('emr')
    
    if event['action'] == 'start':
        # Start cluster with auto-scaling
        response = emr.run_job_flow(
            Name='Scheduled-Cluster',
            ReleaseLabel='emr-6.15.0',
            Instances={
                'MasterInstanceType': 'm5.xlarge',
                'SlaveInstanceType': 'm5.large',
                'InstanceCount': 3,
                'Ec2KeyName': 'your-key'
            },
            Applications=[{'Name': 'Spark'}],
            ServiceRole='EMR_DefaultRole',
            JobFlowRole='EMR_EC2_DefaultRole'
        )
        
        cluster_id = response['JobFlowId']
        
        # Enable managed scaling
        emr.put_managed_scaling_policy(
            ClusterId=cluster_id,
            ManagedScalingPolicy={
                'ComputeLimits': {
                    'UnitType': 'Instances',
                    'MinimumCapacityUnits': 2,
                    'MaximumCapacityUnits': 10
                }
            }
        )
        
        return {'statusCode': 200, 'body': f'Started cluster: {cluster_id}'}
    
    elif event['action'] == 'stop':
        # Terminate cluster
        emr.terminate_job_flows(JobFlowIds=[event['cluster_id']])
        return {'statusCode': 200, 'body': 'Cluster terminated'}
``

### 2. Step Functions Orchestration
Workflow cho complex data pipelines:
``json
{
  "Comment": "EMR Auto-scaling Pipeline",
  "StartAt": "CreateCluster",
  "States": {
    "CreateCluster": {
      "Type": "Task",
      "Resource": "arn:aws:states:::emr:createCluster.sync",
      "Parameters": {
        "Name": "Pipeline-Cluster",
        "ReleaseLabel": "emr-6.15.0"
      },
      "Next": "EnableScaling"
    },
    "EnableScaling": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:emr:putManagedScalingPolicy",
      "Parameters": {
        "ClusterId.$": "$.ClusterId",
        "ManagedScalingPolicy": {
          "ComputeLimits": {
            "UnitType": "Instances",
            "MinimumCapacityUnits": 2,
            "MaximumCapacityUnits": 20
          }
        }
      },
      "Next": "ProcessData"
    },
    "ProcessData": {
      "Type": "Task",
      "Resource": "arn:aws:states:::emr:addStep.sync",
      "End": true
    }
  }
}
``

### 3. EventBridge Rules
Tự động respond to scaling events:
``json
{
  "Rules": [
    {
      "Name": "EMR-ScaleOut-Alert",
      "EventPattern": {
        "source": ["aws.emr"],
        "detail-type": ["EMR Instance Group State Change"],
        "detail": {
          "state": ["RUNNING"],
          "requestedInstanceCount": {
            "numeric": [">", 5]
          }
        }
      },
      "Targets": [
        {
          "Id": "1",
          "Arn": "arn:aws:sns:us-east-1:123456789012:emr-alerts"
        }
      ]
    }
  ]
}
``

## Final Lab: End-to-End Scenario

### Scenario Setup
Tạo một complete data processing pipeline:

1. **Data Ingestion**: S3 → EMR
2. **Processing**: Spark jobs với auto-scaling
3. **Output**: Results → S3
4. **Monitoring**: CloudWatch dashboard
5. **Cleanup**: Automatic termination

### Implementation Steps

1. **Upload Sample Data:**
`aws s3 cp sample-data.csv s3://your-bucket/input/`

2. **Create Processing Script:**
``python
# end-to-end-pipeline.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("E2EPipeline").getOrCreate()

# Read data from S3
df = spark.read.csv("s3://your-bucket/input/sample-data.csv", header=True)

# Heavy processing to trigger scaling
df_processed = df.groupBy("category").agg(
    count("*").alias("count"),
    avg("value").alias("avg_value"),
    max("value").alias("max_value")
).repartition(50)  # Force many partitions

# Cache to consume memory
df_processed.cache()

# Multiple operations
result1 = df_processed.filter(col("count") > 100).count()
result2 = df_processed.orderBy(desc("avg_value")).collect()

# Write results
df_processed.write.mode("overwrite").csv("s3://your-bucket/output/")

print(f"Pipeline completed. Processed {result1} categories")
spark.stop()
``

3. **Submit Pipeline:**
`aws emr add-steps --cluster-id j-xxxxx \
    --steps '[{
        "Name": "E2E-Pipeline",
        "ActionOnFailure": "TERMINATE_CLUSTER",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--executor-memory", "2g",
                "--num-executors", "20",
                "s3://your-bucket/scripts/end-to-end-pipeline.py"
            ]
        }
    }]'`

4. **Monitor Complete Workflow:**
- Cluster starts với 3 instances
- Job bắt đầu, memory usage tăng
- Auto-scaling kicks in, thêm 4-6 instances
- Processing completes
- Scale-in begins, giảm về 3 instances
- Job finishes, cluster có thể terminate

### Expected Timeline
- **0-5 min**: Job startup, initial processing
- **5-10 min**: Heavy load, scaling out to 8-10 instances
- **10-25 min**: Processing với full capacity
- **25-30 min**: Job completion, scaling in
- **30-35 min**: Final cleanup, cluster ready for next job

## Performance Metrics Analysis

### Key Performance Indicators (KPIs)

**Cost Efficiency:**
- Cost per GB processed
- Cost per job completion
- Utilization percentage

**Performance:**
- Job completion time
- Throughput (GB/hour)
- Resource efficiency

**Reliability:**
- Success rate
- Spot interruption impact
- Recovery time

### Benchmarking Results

**Static Cluster (Baseline):**
- 10 instances × 2 hours = 20 instance-hours
- Cost: $1.92
- Processing time: 45 minutes
- Utilization: 60%

**Auto-Scaling Cluster:**
- Average 6 instances × 2 hours = 12 instance-hours
- Cost: $1.15
- Processing time: 50 minutes
- Utilization: 85%

**Improvement:**
- 40% cost reduction
- 25% better utilization
- Only 11% longer processing time

## Graduation Exercise

### Challenge: Optimize Real Workload
Bạn được cung cấp một dataset 10GB và yêu cầu:

1. **Process data với budget tối đa $2**
2. **Complete trong 60 phút**
3. **Achieve 80%+ cluster utilization**
4. **Handle ít nhất 1 spot interruption**

### Solution Approach

1. **Cluster Configuration:**
``json
{
    "InstanceGroups": [
        {
            "Name": "Master",
            "InstanceRole": "MASTER", 
            "InstanceType": "m5.large",
            "InstanceCount": 1,
            "Market": "ON_DEMAND"
        },
        {
            "Name": "Core",
            "InstanceRole": "CORE",
            "InstanceType": "m5.large", 
            "InstanceCount": 1,
            "Market": "ON_DEMAND"
        },
        {
            "Name": "Task",
            "InstanceRole": "TASK",
            "InstanceType": "m5.large",
            "InstanceCount": 0,
            "Market": "SPOT",
            "BidPrice": "0.04"
        }
    ]
}
``

2. **Scaling Policy:**
``json
{
    "ComputeLimits": {
        "UnitType": "Instances",
        "MinimumCapacityUnits": 2,
        "MaximumCapacityUnits": 12,
        "MaximumOnDemandCapacityUnits": 2,
        "MaximumCoreCapacityUnits": 2
    }
}
``

3. **Optimized Spark Job:**
``python
# optimized-processing.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("OptimizedProcessing") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()

# Efficient data reading
df = spark.read.parquet("s3://your-bucket/data/") \
    .repartition(100)  # Optimal partitioning

# Checkpoint để handle spot interruptions
spark.sparkContext.setCheckpointDir("s3://your-bucket/checkpoints/")
df.checkpoint()

# Processing với caching strategy
df_processed = df.groupBy("category", "date") \
    .agg(
        sum("amount").alias("total_amount"),
        count("*").alias("transaction_count"),
        avg("amount").alias("avg_amount")
    ) \
    .cache()

# Multiple outputs để maximize resource usage
df_processed.write.mode("overwrite") \
    .partitionBy("date") \
    .parquet("s3://your-bucket/output/summary/")

df_processed.filter(col("total_amount") > 1000) \
    .write.mode("overwrite") \
    .json("s3://your-bucket/output/high-value/")

spark.stop()
``

### Success Criteria Validation

**Cost Check:**
`aws ce get-cost-and-usage \
    --time-period Start=2024-01-01,End=2024-01-02 \
    --granularity DAILY \
    --metrics BlendedCost \
    --group-by Type=DIMENSION,Key=SERVICE`

**Performance Check:**
- Monitor job completion time
- Check cluster utilization metrics
- Verify data processing accuracy

**Reliability Check:**
- Simulate spot interruption
- Verify job recovery
- Check data consistency

## Kết luận Phần 2

### Những gì đã học được:
- ✅ **EMR Managed Scaling**: Thiết lập và cấu hình
- ✅ **Cost Optimization**: Giảm 40% chi phí với dynamic scaling  
- ✅ **Performance Tuning**: Tối ưu hóa Spark cho auto-scaling
- ✅ **Monitoring**: Theo dõi scaling events và metrics
- ✅ **Troubleshooting**: Xử lý common issues
- ✅ **Production Patterns**: Best practices cho real-world usage

### Tổng kết tiết kiệm chi phí:
- **Phần 1 (Spot)**: 39% savings
- **Phần 2 (Auto Scaling)**: 40% additional savings  
- **Combined**: ~65% total cost reduction

### Next Steps:
Trong Phần 3, chúng ta sẽ thiết lập comprehensive monitoring và alerting system để đảm bảo cluster hoạt động optimal và cost-effective.

{{% notice tip %}}
**Pro Tip**: Trong production, combine auto-scaling với scheduled scaling cho predictable workloads để achieve tối đa 70-80% cost savings!
{{% /notice %}}

---

**Tiếp theo:** [Phần 3: Monitoring & Alerting](/03-monitoring) để complete cost optimization journey.


