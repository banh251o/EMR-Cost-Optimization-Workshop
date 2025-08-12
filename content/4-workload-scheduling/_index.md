---
title: "Part 4: Resource Cleanup"
weight: 40
chapter: false
---

# Part 4: Clean Up Resources - Cost Management

After completing the workshop, it's crucial to clean up all resources to avoid unexpected charges. This section provides comprehensive cleanup procedures for all resources created during the workshop.

## 🎯 Cleanup Objectives

- **Terminate EMR clusters** and associated resources
- **Delete CloudWatch** alarms, dashboards, and custom metrics
- **Remove SNS topics** and subscriptions
- **Clean up Lambda functions** and EventBridge rules
- **Delete S3 buckets** and logs (if created)
- **Verify complete cleanup** to avoid charges

## ⚠️ Important Warnings

{{% notice warning %}}
**CRITICAL**: Deleting resources is irreversible. Ensure you have backed up any important data before proceeding.
{{% /notice %}}

{{% notice info %}}
**Cost Impact**: Leaving resources running can result in significant charges. A single EMR cluster can cost $50-200+ per day.
{{% /notice %}}

## 🧹 Complete Cleanup Guide

### Step 1: EMR Cluster Cleanup

#### 1.1 List All EMR Clusters
```bash
# List all clusters in your account
aws emr list-clusters --active

# List clusters by state
aws emr list-clusters --cluster-states RUNNING WAITING STARTING
```

#### 1.2 Terminate EMR Clusters
```bash
# Terminate specific cluster
aws emr terminate-clusters --cluster-ids j-xxxxx

# Terminate multiple clusters
aws emr terminate-clusters --cluster-ids j-xxxxx j-yyyyy j-zzzzz

# Force terminate if needed
aws emr terminate-clusters --cluster-ids j-xxxxx --force
```

#### 1.3 Verify Cluster Termination
```bash
# Check cluster status
aws emr describe-cluster --cluster-id j-xxxxx --query 'Cluster.Status.State'

# Wait for termination
aws emr wait cluster-terminated --cluster-id j-xxxxx
```

### Step 2: CloudWatch Resources Cleanup

#### 2.1 Delete CloudWatch Alarms
```bash
# List all EMR-related alarms
aws cloudwatch describe-alarms --alarm-name-prefix "EMR-"

# Delete specific alarms
aws cloudwatch delete-alarms --alarm-names \
    "EMR-j-xxxxx-CriticalMemoryUsage" \
    "EMR-j-xxxxx-JobFailures" \
    "EMR-j-xxxxx-ClusterDown" \
    "EMR-j-xxxxx-HourlyCostHigh" \
    "EMR-j-xxxxx-BudgetExceeded"

# Delete all EMR alarms (be careful!)
aws cloudwatch describe-alarms --alarm-name-prefix "EMR-" \
    --query 'MetricAlarms[].AlarmName' --output text | \
    xargs aws cloudwatch delete-alarms --alarm-names
```

#### 2.2 Delete CloudWatch Dashboards
```Bash
# List dashboards
aws cloudwatch list-dashboards --dashboard-name-prefix "EMR-"

# Delete specific dashboard
aws cloudwatch delete-dashboards --dashboard-names "EMR-j-xxxxx-Monitoring"

# Delete all EMR dashboards
aws cloudwatch list-dashboards --dashboard-name-prefix "EMR-" \
    --query 'DashboardEntries[].DashboardName' --output text | \
    xargs aws cloudwatch delete-dashboards --dashboard-names
```

#### 2.3 Clean Up Custom Metrics (Optional)
```bash
# Note: Custom metrics automatically expire after 15 months
# No direct delete command, but they stop incurring charges when not updated

# List custom metrics
aws cloudwatch list-metrics --namespace "EMR/CostOptimization"
```

### Step 3: SNS Topics Cleanup

#### 3.1 List and Delete SNS Topics
# List all SNS topics
aws sns list-topics

# Delete specific topics
```bash
aws sns delete-topic --topic-arn arn:aws:sns:us-east-1:123456789012:emr-critical-alerts
aws sns delete-topic --topic-arn arn:aws:sns:us-east-1:123456789012:emr-warning-alerts
aws sns delete-topic --topic-arn arn:aws:sns:us-east-1:123456789012:emr-info-alerts
aws sns delete-topic --topic-arn arn:aws:sns:us-east-1:123456789012:emr-cost-alerts
```

#### 3.2 Unsubcribe from Topics 
```bash
# List subscriptions
aws sns list-subscriptions

# Unsubscribe
aws sns unsubscribe --subscription-arn arn:aws:sns:us-east-1:123456789012:emr-alerts:xxxxx
```

### Step 4: Lambda Functions Cleanup

#### 4.1 Delete Lambda Functions
```bash
# List Lambda functions
aws lambda list-functions --query 'Functions[?contains(FunctionName, `emr`)].FunctionName'

# Delete specific function
aws lambda delete-function --function-name emr-monitoring

# Delete function with all versions
aws lambda delete-function --function-name emr-monitoring --qualifier '$LATEST'
```

#### 4.2 Delete EventBridge Rules
```bash
# List rules
aws events list-rules --name-prefix "emr-"

# Remove targets first
aws events remove-targets --rule emr-monitoring-schedule --ids "1"

# Delete rule
aws events delete-rule --name emr-monitoring-schedule
```

### Step 5: S3 Resources Cleanup

#### 5.1 Clean Up EMR Logs and Data
```bash
# List S3 buckets used by EMR
aws s3 ls | grep emr

# Delete EMR log files (be careful!)
aws s3 rm s3://your-emr-logs-bucket/ --recursive

# Delete entire bucket (if safe to do so)
aws s3 rb s3://your-emr-logs-bucket --force
```

#### 5.2 Clean Up Bootstrap Scripts
```bash
# Remove bootstrap scripts
aws s3 rm s3://your-bucket/bootstrap/ --recursive
```

### Step 6: IAM Resources Cleanup (Optional)

#### 6.1 Delete Custom IAM Roles (if created)
```bash
# List EMR-related roles
aws iam list-roles --query 'Roles[?contains(RoleName, `EMR`)].RoleName'

# Detach policies first
aws iam detach-role-policy --role-name EMR-Custom-Role --policy-arn arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole

# Delete role
aws iam delete-role --role-name EMR-Custom-Role

```
### Step 7: EC2 Resources Cleanup

#### 7.1 Clean Up Security Groups (if custom created)

```bash
# List EMR security groups
aws ec2 describe-security-groups --filters "Name=group-name,Values=*EMR*"

# Delete custom security group (after cluster termination)
aws ec2 delete-security-group --group-id sg-xxxxx
```

#### 7.2 Clean Up Key Pairs (if created)
```bash
# List key pairs
aws ec2 describe-key-pairs

# Delete key pair
aws ec2 delete-key-pair --key-name emr-workshop-key
```