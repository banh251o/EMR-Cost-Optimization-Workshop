---
title: "Part 1: Spot Instances"
weight: 10
chapter: false
---

# Part 1: Using Spot Instances for Cost Reduction

In this section, you'll learn how to create EMR clusters using Spot Instances to reduce costs by 60-70% compared to On-Demand instances.

## What are Spot Instances?

Spot Instances are unused EC2 instances from AWS, sold at much lower prices than On-Demand. However, AWS can reclaim them anytime when there's demand.

**Advantages:**
- 50-90% cheaper than On-Demand
- Suitable for workloads that can handle interruption
- Well integrated with EMR

**Disadvantages:**
- Can be terminated anytime
- Not suitable for critical workloads
- Need to design applications to handle interruption

## Strategy for Using Spot with EMR

### 1. Mixed Instance Types
Use multiple instance types to increase availability:
- m5.large, m4.large, c5.large
- If one type gets terminated, others continue running

### 2. Role Distribution
- **Master node**: Always use On-Demand (most critical)
- **Core nodes**: Part On-Demand, part Spot
- **Task nodes**: 100% Spot (can terminate without data loss)

### 3. Bidding Strategy
- Set bid price 10-20% higher than current spot price
- Monitor spot price history to set reasonable prices

## Hands-on: Create EMR Cluster with Spot

### Step 1: Preparation
Before creating cluster, you need:
- Login to AWS Console
- Select region (recommended: us-east-1)
- Create EC2 Key Pair if not exists
- Check IAM roles: EMR_DefaultRole and EMR_EC2_DefaultRole

### Step 2: Create Cluster via Console
1. Open AWS Console → EMR
2. Click "Create cluster"
3. Select "Go to advanced options"

**Software Configuration:**
- Release: emr-6.15.0
- Applications: Spark, Hadoop

**Hardware Configuration:**
- Master: 1 x m5.xlarge (On-Demand)
- Core: 1 x m5.large (On-Demand) + 2 x m5.large (Spot, bid $0.05)
- Task: 4 x m5.large (Spot, bid $0.05)

**General Configuration:**
- Cluster name: "Workshop-Spot-Cluster"
- Logging: Enable (select S3 bucket)
- Termination protection: Disable

### Step 3: Monitor Cluster
After creating cluster:
1. Monitor status in EMR Console
2. Check Hardware tab to view instances
3. View Spot price history in EC2 Console

### Step 4: Cost Comparison

**On-Demand Configuration:**
- Master: 1 x m5.xlarge = $0.192/hour
- Workers: 6 x m5.large = $0.576/hour
- **Total: $0.768/hour**

**Spot Configuration:**
- Master: 1 x m5.xlarge = $0.192/hour
- Core On-Demand: 1 x m5.large = $0.096/hour
- Spot instances: 6 x m5.large = $0.180/hour (assuming spot price $0.03)
- **Total: $0.468/hour**

**Savings: 39% ($0.30/hour)**

## Handling Spot Interruption

### Monitoring Interruptions
EMR automatically handles spot interruptions:
- Task nodes terminated: Jobs are redistributed
- Core nodes terminated: Data is replicated
- Cluster continues running with remaining instances

### Best Practices
1. **Frequent checkpointing**: Save intermediate results
2. **Use S3**: Store data outside cluster
3. **Mixed instance types**: Reduce risk of all being terminated together
4. **Monitor spot prices**: Adjust bid prices when needed

## Lab Exercise: Test Spot Interruption

### Create Test Job
1. SSH to master node
2. Create test job file:

```python
# simple-job.py
from pyspark.sql import SparkSession
import time

spark = SparkSession.builder.appName("SpotTest").getOrCreate()

# Create large dataset
df = spark.range(0, 10000000).toDF("id")
df = df.repartition(100)

# Run long job to test interruption
for i in range(10):
    result = df.count()
    print(f"Iteration {i}: Count = {result}")
    time.sleep(60)  # Wait 1 minute

spark.stop()


Submit job: spark-submit simple-job.py
Simulate Interruption
In EC2 Console, terminate a spot instance
Observe job continues running
Check EMR Console for cluster status
Part 1 Results
After completing this section, you have:

✅ Understood how Spot Instances work with EMR
✅ Created cluster with mixed instance types
✅ Saved 39% cost compared to On-Demand
✅ Tested spot interruption handling
Actual cost: Cluster running 1 hour = ~$0.47 (instead of $0.77)

{{% notice success %}} Congratulations! You've successfully created an EMR cluster with Spot Instances and achieved significant cost savings. Next, we'll learn Auto Scaling for further optimization. {{% /notice %}}

{{% notice warning %}} Note: Don't terminate the cluster yet! We'll use this cluster for Part 2: Auto Scaling. {{% /notice %}}

Frequently Asked Questions
Q: Are spot instances reliable? A: With EMR, spot instances are very reliable because EMR automatically handles interruptions. Just need to design jobs properly.

Q: When should I use Spot instances? A: Spot instances are suitable for batch processing, data analysis, machine learning training - workloads that can be restarted.

Q: How do I know the right bid price? A: Check Spot Price History in EC2 Console, set bid 10-20% higher than average price.

Q: What if all spot instances get terminated? A: EMR will automatically launch new instances. Jobs can restart from the nearest checkpoint.

Next: Part 2: Auto Scaling  to learn how to automatically adjust cluster size based on workload.