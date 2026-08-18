# EdgeTrust production infrastructure.
#
# Provisions the pieces this repo's local dev setup stands in for:
#   - a managed Redpanda cluster (event streaming backbone)
#   - a Kubernetes cluster to run the Flink job and the Go scoring service
#   - a managed Redis (ElastiCache) instance as the online feature store
#
# Not applied in this build environment (no cloud credentials, no network
# egress to AWS endpoints), same as the Databricks Terraform in the Bank
# Lakehouse Migration project. This is the actual deployment target the
# local components in this repo are written against.

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.30"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

resource "aws_elasticache_cluster" "feature_store" {
  cluster_id           = "edgetrust-feature-store"
  engine               = "redis"
  node_type            = "cache.r6g.large"
  num_cache_nodes      = 1
  parameter_group_name = "default.redis7"
  port                 = 6379
  tags = {
    Project = "edgetrust"
  }
}

resource "aws_eks_cluster" "edgetrust" {
  name     = "edgetrust-cluster"
  role_arn = aws_iam_role.eks_cluster.arn

  vpc_config {
    subnet_ids = var.subnet_ids
  }
}

resource "aws_iam_role" "eks_cluster" {
  name = "edgetrust-eks-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "eks.amazonaws.com" }
    }]
  })
}

# Redpanda is deployed via the Redpanda Helm chart into the EKS cluster
# above rather than provisioned directly here; see infra/redpanda_values.yaml
# for the Helm release configuration.

variable "aws_region" {
  default = "us-east-1"
}

variable "subnet_ids" {
  type = list(string)
}
