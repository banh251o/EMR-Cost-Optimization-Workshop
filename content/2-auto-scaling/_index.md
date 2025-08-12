---
title: "Part 2: Auto Scaling"
weight: 20
chapter: false
---

# Part 2: EMR Auto Scaling - Dynamic Workload Adjustment

In this section, you'll learn how to set up EMR Auto Scaling to automatically adjust the number of instances in your cluster based on workload, optimizing both cost and performance.

## What is EMR Auto Scaling?

EMR Auto Scaling automatically adjusts the number of instances in your cluster based on:
- **YARN metrics**: Memory and CPU utilization
- **Custom metrics**: Custom CloudWatch metrics
- **Time-based scaling**: Scheduled scaling patterns

**Benefits:**
- Cost savings during low workload periods
- Improved performance during high workload periods
- No manual intervention required
- Great integration with Spot Instances

## Types of Auto Scaling

### 1. EMR Managed Scaling (Recommended)
- Fully managed by AWS
- Based on YARN container pending metrics
- Simple and effective
- Supports both On-Demand and Spot instances

### 2. Custom Auto Scaling
- Define your own scaling policies
- Based on CloudWatch metrics
- More flexible but complex
- Suitable for special use cases

## Hands-on: Setting up Managed Scaling

### Step 1: Enable Managed Scaling
Using the cluster from Part 1, we'll enable auto scaling:

1. Open EMR Console
2. Select cluster "Workshop-Spot-Cluster"
3. Go to "Configuration" tab
4. Click "Edit" in the "Scaling" section

**Managed Scaling Configuration:**
- Minimum capacity: 2 instances
- Maximum capacity: 10 instances
- Maximum On-Demand capacity: 4 instances

### Step 2: Configure Advanced Settings
**Scale-out settings:**
- Scale out cooldown: 300 seconds
- Maximum scale-out increment: 100%

**Scale-in settings:**
- Scale in cooldown: 300 seconds
- Maximum scale-in increment: 50%

### Step 3: Verify Configuration
After enabling, check:
- Scaling status: "Enabled"
- Current capacity: 7 instances (from Part 1)
- Target capacity: Will change based on workload

## Testing Auto Scaling

### Create Workload to Test Scaling

1. **SSH to Master Node:**
`ssh -i your-key.pem hadoop@master-public-ip`

2. **Create Test Script:**
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

# Create large dataset to trigger scaling
print("Creating large dataset...")
df = spark.range(0, 100000000).toDF("id")
df = df.repartition(200)  # Many partitions to require more resources

# Cache to consume memory
df.cache()

# Run multiple operations to maintain load
for i in range(5):
    print(f"Running operation {i+1}/5...")
    
    # Heavy computation
    result = df.filter(df.id % 2 == 0).count()
    print(f"Even numbers count: {result}")
    
    # Keep load for 5 minutes to observe scaling
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

### Monitor Scaling Process

**In EMR Console:**
1. Go to "Hardware" tab to view instances
2. Refresh every 2-3 minutes
3. Observe instance count increasing

**In CloudWatch:**
1. Open CloudWatch Console
2. Go to "Metrics" → "AWS/ElasticMapReduce"
3. Select metrics:
   - YARNMemoryAvailablePercentage
   - ContainerPending
   - AppsRunning

**Expected Behavior:**
- **Minutes 0-2**: Job starts, YARN memory decreases
- **Minutes 2-5**: ContainerPending increases, scaling triggered
- **Minutes 5-8**: New instances added (2-3 instances)
- **Minutes 8-25**: Job runs with new capacity
- **Minutes 25-30**: Job completes, scaling down begins

## Monitoring and Troubleshooting

### Key Metrics to Monitor

**YARN Metrics:**
- **YARNMemoryAvailablePercentage**: < 15% triggers scale out
- **ContainerPending**: > 0 for 5 minutes triggers scale out
- **AppsRunning**: Number of running applications

**EMR Metrics:**
- **RunningMapTasks**: Running map tasks
- **RunningReduceTasks**: Running reduce tasks
- **TotalLoad**: Total cluster load

### Common Issues and Solutions

**Issue 1: Scaling not working**
- Check IAM permissions
- Verify scaling limits (min/max capacity)
- Check cooldown periods

**Issue 2: Scale out too slow**
- Reduce scale-out cooldown
- Increase maximum scale-out increment
- Use multiple instance types

**Issue 3: Scale in too aggressive**
- Increase scale-in cooldown
- Reduce maximum scale-in increment
- Adjust YARN memory thresholds

## Advanced Scaling Strategies

### 1. Mixed Instance Types for Scaling
Configure multiple instance types to increase availability:

**Instance Fleet Configuration:**
- Primary: m5.large (Spot)
- Secondary: m4.large (Spot)
- Fallback: c5.large (Spot)
- Emergency: m5.large (On-Demand)

### 2. Time-based Scaling
For workloads with fixed patterns:
- Scale out before peak hours
- Scale in after off-peak hours
- Use CloudWatch Events + Lambda

### 3. Predictive Scaling
Based on historical data:
- Analyze past workload patterns
- Pre-scale for expected load
- Combine with reactive scaling

## Cost Optimization with Auto Scaling

### Before Auto Scaling (Static Cluster)
- **Peak capacity**: 10 instances × 8 hours = 80 instance-hours
- **Cost**: 80 × $0.096 = $7.68/day

### After Auto Scaling (Dynamic Cluster)
- **Average capacity**: 4 instances × 8 hours = 32 instance-hours
- **Peak capacity**: 8 instances × 2 hours = 16 instance-hours
- **Total**: 32 + 16 = 48 instance-hours
- **Cost**: 48 × $0.096 = $4.61/day

**Savings**: $3.07/day (40% cost reduction)

## Lab Exercise: Custom Scaling Policy

### Create Custom CloudWatch Alarm

1. **Create Scale-Out Alarm:**
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

2. **Create Scale-In Alarm:**
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

1. **Create Light Workload:**
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

2. **Submit and Monitor:**
`spark-submit light-workload.py`

3. **Observe Scale-In:**
- Memory usage drops below 30%
- After 10 minutes, cluster scales in
- Instances reduce from 8 to 4

## Production Best Practices

### 1. Scaling Configuration
**Recommended Settings:**
- Min capacity: 20% of peak capacity
- Max capacity: 150% of expected peak
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
- Graceful handling of node loss
- Appropriate data partitioning

### 4. Monitoring Setup
**Essential Metrics:**
- Cluster utilization
- Scaling events
- Job completion times
- Cost per job

## Part 2 Results

After completing this section, you have:
- ✅ Successfully set up EMR Managed Scaling
- ✅ Tested scaling with real workload
- ✅ Understood how to monitor scaling metrics
- ✅ Optimized an additional 40% cost with dynamic scaling

**Total savings so far:**
- Spot Instances: 39% (from Part 1)
- Auto Scaling: 40% (from Part 2)
- **Combined savings**: ~65% compared to static On-Demand cluster

{{% notice success %}}
**Excellent!** Your cluster now automatically scales based on workload and maximizes cost savings. Next, we'll set up monitoring to track everything.
{{% /notice %}}

## Troubleshooting Common Issues

### Issue: Scaling Events Not Appearing
**Possible Causes:**
- IAM role missing permissions
- Cooldown period not expired
- Metrics not reaching threshold

**Solutions:**
1. Check CloudTrail logs
2. Verify EMR service role permissions
3. Adjust threshold values

### Issue: Scale-in Too Aggressive
**Symptoms:**
- Instances terminated while jobs running
- Performance degradation

**Solutions:**
1. Increase scale-in cooldown to 900s
2. Reduce maximum scale-in increment to 25%
3. Set higher memory threshold (80% instead of 70%)

### Issue: Spot Instances Not Added During Scaling
**Causes:**
- Spot capacity not available
- Bid price too low
- Instance type constraints

**Solutions:**
1. Add multiple instance types
2. Increase bid price
3. Enable multiple AZs

## Performance Tuning Tips

### 1. Optimize Spark Configuration
`# Spark configs for auto-scaling environment
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
- Use cluster mode instead of client mode
- Enable speculation for fault tolerance
- Configure appropriate parallelism

## Real-world Example: E-commerce Analytics

### Scenario
E-commerce company needs to process daily log data:
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
Create CloudWatch dashboard with:
- Cluster capacity over time
- Cost per hour tracking
- Job completion rates
- Scaling events timeline

### Automated Alerts
Setup alerts for:
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

## Integration with Other AWS Services

### 1. Lambda Triggers
Automatically start/stop clusters:
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
Workflow for complex data pipelines:
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
Automatically respond to scaling events:
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
Create a complete data processing pipeline:

1. **Data Ingestion**: S3 → EMR
2. **Processing**: Spark jobs with auto-scaling
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
- Cluster starts with 3 instances
- Job begins, memory usage increases
- Auto-scaling kicks in, adds 4-6 instances
- Processing completes
- Scale-in begins, reduces to 3 instances
- Job finishes, cluster can terminate

### Expected Timeline
- **0-5 min**: Job startup, initial processing
- **5-10 min**: Heavy load, scaling out to 8-10 instances
- **10-25 min**: Processing with full capacity
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
You are provided with a 10GB dataset and requirements:

1. **Process data with maximum budget of $2**
2. **Complete within 60 minutes**
3. **Achieve 80%+ cluster utilization**
4. **Handle at least 1 spot interruption**

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

# Checkpoint to handle spot interruptions
spark.sparkContext.setCheckpointDir("s3://your-bucket/checkpoints/")
df.checkpoint()

# Processing with caching strategy
df_processed = df.groupBy("category", "date") \
    .agg(
        sum("amount").alias("total_amount"),
        count("*").alias("transaction_count"),
        avg("amount").alias("avg_amount")
    ) \
    .cache()

# Multiple outputs to maximize resource usage
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

## Advanced Auto Scaling Techniques

### 1. Predictive Scaling with Machine Learning
``python
# predictive-scaling.py
import boto3
import pandas as pd
from sklearn.linear_model import LinearRegression
import numpy as np

class PredictiveScaler:
    def __init__(self, cluster_id):
        self.cluster_id = cluster_id
        self.cloudwatch = boto3.client('cloudwatch')
        self.emr = boto3.client('emr')
    
    def get_historical_metrics(self, days=30):
        # Get historical YARN metrics
        end_time = datetime.now()
        start_time = end_time - timedelta(days=days)
        
        response = self.cloudwatch.get_metric_statistics(
            Namespace='AWS/ElasticMapReduce',
            MetricName='YARNMemoryAvailablePercentage',
            Dimensions=[
                {'Name': 'JobFlowId', 'Value': self.cluster_id}
            ],
            StartTime=start_time,
            EndTime=end_time,
            Period=3600,
            Statistics=['Average']
        )
        
        return response['Datapoints']
    
    def predict_scaling_needs(self):
        # Simple ML model for prediction
        data = self.get_historical_metrics()
        df = pd.DataFrame(data)
        
        if len(df) < 24:  # Need at least 24 hours of data
            return None
        
        # Feature engineering
        df['hour'] = df['Timestamp'].dt.hour
        df['day_of_week'] = df['Timestamp'].dt.dayofweek
        
        # Train model
        X = df[['hour', 'day_of_week']]
        y = df['Average']
        
        model = LinearRegression()
        model.fit(X, y)
        
        # Predict next hour
        next_hour = datetime.now().hour + 1
        next_day = datetime.now().weekday()
        
        predicted_usage = model.predict([[next_hour, next_day]])[0]
        
        # Recommend scaling action
        if predicted_usage < 20:  # Low memory available
            return 'scale_out'
        elif predicted_usage > 80:  # High memory available
            return 'scale_in'
        else:
            return 'no_action'
    
    def apply_predictive_scaling(self):
        action = self.predict_scaling_needs()
        
        if action == 'scale_out':
            # Pre-emptively scale out
            self.emr.put_managed_scaling_policy(
                ClusterId=self.cluster_id,
                ManagedScalingPolicy={
                    'ComputeLimits': {
                        'UnitType': 'Instances',
                        'MinimumCapacityUnits': 4,  # Increase minimum
                        'MaximumCapacityUnits': 15
                    }
                }
            )
        elif action == 'scale_in':
            # Allow more aggressive scale-in
            self.emr.put_managed_scaling_policy(
                ClusterId=self.cluster_id,
                ManagedScalingPolicy={
                    'ComputeLimits': {
                        'UnitType': 'Instances',
                        'MinimumCapacityUnits': 2,  # Reduce minimum
                        'MaximumCapacityUnits': 10
                    }
                }
            )

# Usage
scaler = PredictiveScaler('j-xxxxx')
scaler.apply_predictive_scaling()
``

### 2. Multi-Cluster Auto Scaling
For very large workloads, manage multiple clusters:

``python
# multi-cluster-manager.py
import boto3
import json
from datetime import datetime

class MultiClusterManager:
    def __init__(self):
        self.emr = boto3.client('emr')
        self.cloudwatch = boto3.client('cloudwatch')
        self.max_clusters = 3
        self.cluster_template = {
            'Name': 'Auto-Cluster',
            'ReleaseLabel': 'emr-6.15.0',
            'Applications': [{'Name': 'Spark'}],
            'ServiceRole': 'EMR_DefaultRole',
            'JobFlowRole': 'EMR_EC2_DefaultRole'
        }
    
    def get_active_clusters(self):
        response = self.emr.list_clusters(
            ClusterStates=['STARTING', 'BOOTSTRAPPING', 'RUNNING', 'WAITING']
        )
        return [cluster for cluster in response['Clusters'] 
                if 'Auto-Cluster' in cluster['Name']]
    
    def get_cluster_load(self, cluster_id):
        try:
            response = self.cloudwatch.get_metric_statistics(
                Namespace='AWS/ElasticMapReduce',
                MetricName='ContainerPending',
                Dimensions=[{'Name': 'JobFlowId', 'Value': cluster_id}],
                StartTime=datetime.now() - timedelta(minutes=10),
                EndTime=datetime.now(),
                Period=300,
                Statistics=['Average']
            )
            
            if response['Datapoints']:
                return response['Datapoints'][-1]['Average']
            return 0
        except:
            return 0
    
    def should_create_new_cluster(self):
        active_clusters = self.get_active_clusters()
        
        if len(active_clusters) >= self.max_clusters:
            return False
        
        # Check if all clusters are heavily loaded
        total_pending = 0
        for cluster in active_clusters:
            pending = self.get_cluster_load(cluster['Id'])
            total_pending += pending
        
        # Create new cluster if average pending > 10
        avg_pending = total_pending / len(active_clusters) if active_clusters else 0
        return avg_pending > 10
    
    def create_cluster(self):
        response = self.emr.run_job_flow(
            **self.cluster_template,
            Instances={
                'MasterInstanceType': 'm5.large',
                'SlaveInstanceType': 'm5.large',
                'InstanceCount': 3,
                'Ec2KeyName': 'your-key',
                'InstanceGroups': [
                    {
                        'Name': 'Master',
                        'InstanceRole': 'MASTER',
                        'InstanceType': 'm5.large',
                        'InstanceCount': 1,
                        'Market': 'ON_DEMAND'
                    },
                    {
                        'Name': 'Core',
                        'InstanceRole': 'CORE',
                        'InstanceType': 'm5.large',
                        'InstanceCount': 2,
                        'Market': 'SPOT',
                        'BidPrice': '0.05'
                    }
                ]
            }
        )
        
        cluster_id = response['JobFlowId']
        
        # Enable auto-scaling on new cluster
        self.emr.put_managed_scaling_policy(
            ClusterId=cluster_id,
            ManagedScalingPolicy={
                'ComputeLimits': {
                    'UnitType': 'Instances',
                    'MinimumCapacityUnits': 3,
                    'MaximumCapacityUnits': 10
                }
            }
        )
        
        return cluster_id
    
    def terminate_idle_clusters(self):
        active_clusters = self.get_active_clusters()
        
        for cluster in active_clusters:
            # Check if cluster is idle
            steps = self.emr.list_steps(
                ClusterId=cluster['Id'],
                StepStates=['RUNNING', 'PENDING']
            )
            
            if not steps['Steps']:
                # No running steps, check idle time
                cluster_details = self.emr.describe_cluster(ClusterId=cluster['Id'])
                ready_time = cluster_details['Cluster']['Status']['Timeline'].get('ReadyDateTime')
                
                if ready_time:
                    idle_minutes = (datetime.now() - ready_time).total_seconds() / 60
                    if idle_minutes > 30:  # Idle for 30 minutes
                        self.emr.terminate_job_flows(JobFlowIds=[cluster['Id']])
                        print(f"Terminated idle cluster: {cluster['Id']}")
    
    def manage_clusters(self):
        # Create new cluster if needed
        if self.should_create_new_cluster():
            new_cluster = self.create_cluster()
            print(f"Created new cluster: {new_cluster}")
        
        # Terminate idle clusters
        self.terminate_idle_clusters()

# Schedule this to run every 5 minutes
manager = MultiClusterManager()
manager.manage_clusters()
``

### 3. Cost-Aware Scaling
Implement scaling that considers cost constraints:

``python
# cost-aware-scaling.py
import boto3
from datetime import datetime, timedelta

class CostAwareScaler:
    def __init__(self, cluster_id, daily_budget=50):
        self.cluster_id = cluster_id
        self.daily_budget = daily_budget
        self.emr = boto3.client('emr')
        self.ce = boto3.client('ce')
        self.cloudwatch = boto3.client('cloudwatch')
    
    def get_current_spend(self):
        today = datetime.now().strftime('%Y-%m-%d')
        
        response = self.ce.get_cost_and_usage(
            TimePeriod={
                'Start': today,
                'End': today
            },
            Granularity='DAILY',
            Metrics=['BlendedCost'],
            GroupBy=[
                {'Type': 'DIMENSION', 'Key': 'SERVICE'}
            ]
        )
        
        emr_cost = 0
        for result in response['ResultsByTime']:
            for group in result['Groups']:
                if 'ElasticMapReduce' in group['Keys'][0]:
                    emr_cost += float(group['Metrics']['BlendedCost']['Amount'])
        
        return emr_cost
    
    def calculate_hourly_cost(self):
        instances = self.emr.list_instances(
            ClusterId=self.cluster_id,
            InstanceStates=['RUNNING']
        )
        
        hourly_cost = 0
        for instance in instances['Instances']:
            instance_type = instance['InstanceType']
            market = instance.get('Market', 'ON_DEMAND')
            
            # Simplified cost calculation
            if 'm5.large' in instance_type:
                cost = 0.096 if market == 'ON_DEMAND' else 0.035
            elif 'm5.xlarge' in instance_type:
                cost = 0.192 if market == 'ON_DEMAND' else 0.070
            else:
                cost = 0.1
            
            hourly_cost += cost
        
        return hourly_cost
    
    def can_scale_out(self, additional_instances=1):
        current_spend = self.get_current_spend()
        hourly_cost = self.calculate_hourly_cost()
        
        # Estimate cost for additional instances
    def can_scale_out(self, additional_instances=1):
        current_spend = self.get_current_spend()
        hourly_cost = self.calculate_hourly_cost()
        
        # Estimate cost for additional instances
        additional_cost = additional_instances * 0.035  # Spot price
        hours_remaining = 24 - datetime.now().hour
        
        projected_spend = current_spend + (hourly_cost + additional_cost) * hours_remaining
        
        return projected_spend <= self.daily_budget * 0.9  # 90% of budget
    
    def smart_scale_decision(self):
        # Get current metrics
        response = self.cloudwatch.get_metric_statistics(
            Namespace='AWS/ElasticMapReduce',
            MetricName='ContainerPending',
            Dimensions=[{'Name': 'JobFlowId', 'Value': self.cluster_id}],
            StartTime=datetime.now() - timedelta(minutes=10),
            EndTime=datetime.now(),
            Period=300,
            Statistics=['Average']
        )
        
        pending_containers = 0
        if response['Datapoints']:
            pending_containers = response['Datapoints'][-1]['Average']
        
        # Scale out decision
        if pending_containers > 5 and self.can_scale_out():
            return 'scale_out'
        elif pending_containers == 0:
            return 'scale_in'
        else:
            return 'no_action'
    
    def apply_cost_aware_scaling(self):
        decision = self.smart_scale_decision()
        current_spend = self.get_current_spend()
        budget_used = (current_spend / self.daily_budget) * 100
        
        print(f"Budget used: {budget_used:.1f}%")
        print(f"Scaling decision: {decision}")
        
        if decision == 'scale_out' and budget_used < 80:
            # Conservative scaling when budget is tight
            max_capacity = 15 if budget_used < 50 else 8
            
            self.emr.put_managed_scaling_policy(
                ClusterId=self.cluster_id,
                ManagedScalingPolicy={
                    'ComputeLimits': {
                        'UnitType': 'Instances',
                        'MinimumCapacityUnits': 3,
                        'MaximumCapacityUnits': max_capacity
                    }
                }
            )
        elif decision == 'scale_in' or budget_used > 90:
            # Aggressive scale-in when over budget
            self.emr.put_managed_scaling_policy(
                ClusterId=self.cluster_id,
                ManagedScalingPolicy={
                    'ComputeLimits': {
                        'UnitType': 'Instances',
                        'MinimumCapacityUnits': 2,
                        'MaximumCapacityUnits': 5
                    }
                }
            )

# Usage
scaler = CostAwareScaler('j-xxxxx', daily_budget=100)
scaler.apply_cost_aware_scaling()
``

## Production Deployment Checklist

### Pre-Deployment
- [ ] IAM roles configured with proper permissions
- [ ] VPC and security groups set up
- [ ] S3 buckets created for data and logs
- [ ] CloudWatch alarms configured
- [ ] SNS topics for notifications
- [ ] Lambda functions for automation

### Scaling Configuration
- [ ] Minimum capacity set to handle baseline load
- [ ] Maximum capacity set within budget constraints
- [ ] Cooldown periods optimized for workload
- [ ] Instance types diversified for availability
- [ ] Spot/On-Demand mix configured

### Monitoring Setup
- [ ] CloudWatch dashboard created
- [ ] Cost tracking enabled
- [ ] Performance metrics monitored
- [ ] Alert thresholds configured
- [ ] Log aggregation set up

### Testing Validation
- [ ] Scale-out tested with heavy workload
- [ ] Scale-in tested with light workload
- [ ] Spot interruption handling verified
- [ ] Cost limits enforced
- [ ] Performance benchmarks established

## Conclusion Part 2

### What You've Accomplished:
- ✅ **EMR Managed Scaling**: Set up and configured successfully
- ✅ **Cost Optimization**: Achieved 40% cost reduction with dynamic scaling  
- ✅ **Performance Tuning**: Optimized Spark for auto-scaling environment
- ✅ **Monitoring**: Set up scaling events and metrics tracking
- ✅ **Troubleshooting**: Learned to handle common scaling issues
- ✅ **Production Patterns**: Implemented best practices for real-world usage

### Cost Savings Summary:
- **Part 1 (Spot)**: 39% savings
- **Part 2 (Auto Scaling)**: 40% additional savings  
- **Combined**: ~65% total cost reduction compared to static On-Demand cluster

### Key Takeaways:
1. **Auto-scaling works best with**: Predictable workload patterns, proper monitoring, and cost constraints
2. **Spot + Auto-scaling combination**: Provides maximum cost efficiency
3. **Monitoring is critical**: For troubleshooting and optimization
4. **Application design matters**: Stateless, fault-tolerant applications scale better

### Real-World Impact:
- **Small company (10 clusters)**: $2,000/month → $700/month = $1,300 saved
- **Medium company (50 clusters)**: $10,000/month → $3,500/month = $6,500 saved
- **Large enterprise (200 clusters)**: $40,000/month → $14,000/month = $26,000 saved

### Next Steps:
In Part 3, we'll set up comprehensive monitoring and alerting to ensure your optimized clusters run smoothly and cost-effectively in production.

{{% notice tip %}}
**Pro Tip**: In production, combine auto-scaling with scheduled scaling for predictable workloads to achieve maximum 70-80% cost savings!
{{% /notice %}}

---

**Next:** [Part 3: Monitoring & Alerting](/03-monitoring) to complete your cost optimization journey.
