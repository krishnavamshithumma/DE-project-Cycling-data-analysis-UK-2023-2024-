# Cycling Traffic Analysis: UK (2023-2024)

[![GCP](https://img.shields.io/badge/Google_Cloud-4285F4?style=for-the-badge&logo=google-cloud&logoColor=white)](https://cloud.google.com)
[![Terraform](https://img.shields.io/badge/Terraform-7B42BC?style=for-the-badge&logo=terraform&logoColor=white)](https://www.terraform.io/)
[![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org)

## 🚲 Problem Statement
Urban mobility planners need granular insights into cycling patterns to optimize infrastructure investments. This project analyzes 2 years of UK cycling traffic data (2023-2024) to identify:
- Weather impact on cycling volumes
- Popular routes/modes of transportation

Provides actionable insights for city planners to:
✅ Improve bicycle lane safety  
✅ Optimize maintenance schedules  
✅ Plan future micromobility infrastructure  

## 🛠️ Prerequisites
1. **Google Cloud Platform Account** (Free Tier eligible)
2. **Terraform** (v1.5.0+)
3. **Python** (3.8+)
4. **Apache Airflow** (2.6.0+)

## 🚀 Deployment Architecture
![Architecture Diagram](https://via.placeholder.com/800x400.png?text=GCP+Architecture+Diagram) <!-- Replace with actual diagram -->

## ⚙️ Setup Guide

### 1. Infrastructure Setup (Terraform)
```bash
# Clone repository
git clone https://github.com/yourusername/cycling-data-analysis-uk.git
cd terraform/

# Initialize Terraform
terraform init

# Plan infrastructure
terraform plan

# Apply configuration
terraform apply
```
**Critical Configurations:**  
Update `variables.tf` with:
- GCP bucket name
- BigQuery dataset ID
- Service account credentials path  
Store service account JSON in `/terraform/keys/`

### 2. Data Pipeline (Airflow)
```bash
# Initialize Airflow DB
airflow db init

# Start scheduler
airflow scheduler

# Start webserver (new terminal)
airflow webserver --port 8080
```
**DAG Configuration:**  
- Place `airflow_web_to_gcs.py` in `~/airflow/dags/`
- Update GCS bucket path in DAG file  

**Trigger pipeline:**  
```bash
airflow dags trigger cycling_data_download_to_gcs
```

### 3. Data Transformation (Dataproc)
```bash
# Create Dataproc cluster
gcloud dataproc clusters create cycling-cluster \
    --region=us-central1 \
    --single-node \
    --service-account=<YOUR_SERVICE_ACCOUNT>

# Submit Spark job
gcloud dataproc jobs submit pyspark \
    --cluster=cycling-cluster \
    --region=us-central1 \
    --jars=gs://spark-lib/bigquery/spark-3.5-bigquery-0.41.1.jar \
    gs://<YOUR_BUCKET>/code/bigquery.py \
    -- \
        --input_2023=gs://<BUCKET>/cyclingdata_2023/* \
        --input_2024=gs://<BUCKET>/cyclingdata_2024/* \
        --output=<DATASET_ID>.factdata_all
```
Before you run the spark job. Make sure :

- once it is created, in the GCS Bucket section, you will see other buckets apart from the one you created, replace the dataproc temp bucket in the bigquery.py file with your temp dataproc bucket id.

- put the bigquery.py file in your original gcs bucket in an other folder called "code".
```bash
gsutil cp bigquery.py gs://<BUCKET>/code/
```

## 💊 Visualization Example
Sample Visualization <!-- Add actual screenshot -->

## 🧹 Cleanup
```bash
# Destroy Terraform resources
terraform destroy

# Delete Dataproc cluster
gcloud dataproc clusters delete cycling-cluster --region=us-central1
```

## 📝 License
This project is licensed under the MIT License - see LICENSE.md for details.

## 🤝 Contributing
Pull requests welcome! Please follow:
1. Fork the repository
2. Create your feature branch
3. Commit changes
4. Push to the branch
5. Open a PR

