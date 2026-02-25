# Topic Classification Precision Improvements

**Date**: 2026-02-10
**Issue**: Brief report indicated topic-label mismatch and off-topic evidence in high-score buckets

## Problems Identified

1. **Over-broad keyword matching**: Generic terms like "AWS", "container", "serverless" matched content where they were mentioned tangentially
2. **Short acronym false positives**: "RAG", "EKS" matched in wrong contexts (e.g., "RAG" as Red-Amber-Green status)
3. **Generic fallback pollution**: `aws.general`, `ai.general`, `cloud.general` were matching too frequently and crowding out specific topics
4. **Missing specific services**: No `aws.ecs` or `aws.fargate` topics to capture container orchestration signals

## Changes Made

### 1. Restricted Generic Topics (Priority 10-20)

- **`aws.general`** (priority 30 → 20):
  - Removed broad "AWS" keyword matcher
  - Now only matches "Amazon Web Services" (full name) or AWS platform/strategy contexts
  - Example: Won't match "AWS ECS announcement" (should match `aws.ecs` instead)

- **`ai.general`** (priority 25 → 15):
  - Removed simple "AI/ML" keyword matchers
  - Now requires substantive context: "AI breakthrough", "machine learning algorithm"
  - Won't match passing AI mentions

- **`cloud.general`** (priority 30 → 10):
  - Now requires strategic context: "multi-cloud strategy", "cloud-native architecture"
  - Won't match every "cloud" mention

### 2. Added Context Requirements for Acronyms

- **`aws.eks`** (priority 80):
  - Now requires EKS in AWS context OR with K8s-specific terms (cluster/node/pod)
  - Example: Won't match standalone "EKS" without clear AWS/K8s context

- **`ai.rag`** (priority 80):
  - Now requires RAG with AI/retrieval context
  - Example: Won't match "RAG status" (Red-Amber-Green compliance)
  - Will match "RAG with vector embeddings" or "retrieval-augmented generation"

### 3. Removed Over-Broad Keywords

- **`aws.lambda`**: Removed generic "serverless" keyword (too many false positives)
- **`cloud.docker`**: Removed generic "container" keyword (matches ECS, K8s contexts incorrectly)

### 4. Added Missing Specific Topics

- **`aws.ecs`** (priority 80): AWS Elastic Container Service
  - Matchers: "ECS", "Elastic Container Service", "ECS task/service/cluster"

- **`aws.fargate`** (priority 75): AWS Fargate
  - Matcher: "Fargate"

### 5. Updated Priority Ranges

```
90-100: Highest - specific AI services, critical infra (Bedrock, Agents)
75-85:  High - specific cloud services, major platforms (EC2, ECS, SageMaker)
60-70:  Medium - supporting tools, frameworks (Kubernetes, Terraform)
40-55:  Low-Medium - languages, databases, general dev tools
20-30:  Low - broad services (aws.general)
10-15:  Lowest - catch-all categories (ai.general, cloud.general)
```

## Verification

All 21 topic extraction tests pass, including 7 new precision tests:

✅ Should NOT match `aws.general` for tangential AWS mentions
✅ Should match `aws.ec2` and `aws.ecs` specifically for ECS/EC2 content
✅ Should NOT match `ai.rag` for random RAG abbreviation
✅ Should match `ai.rag` with proper context
✅ Should NOT match `cloud.docker` for generic container mentions
✅ Should prefer specific topics over general fallbacks
✅ Should require EKS in AWS context to avoid false positives

## Expected Impact

1. **Higher signal-to-noise ratio**: Generic topics will only appear when truly warranted
2. **More accurate trend detection**: Specific service topics (ECS, EC2, Fargate) will capture granular movements
3. **Fewer topic-label mismatches**: Context requirements reduce false positives
4. **Better brief quality**: Score-based intelligence will reflect actual engineering movement, not tangential mentions

## Testing the Improvements

Restart the Collector to load the updated allowlist:

```bash
docker compose restart collector
```

Generate a new brief after 24-48 hours of data collection:

```bash
docker compose exec ops-cli riops brief trigger
```

The brief should show:
- More specific AWS services (ecs, ec2, fargate) instead of aws.general
- More targeted AI topics instead of ai.general
- Better topic-label alignment overall
