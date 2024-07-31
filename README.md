# Raw Data Migration Project

## Overview

This project focuses on extracting NoSQL JSON data from Amazon S3, transforming it based on specific requirements, and saving the processed data into Amazon RDS. The purpose of this project is to streamline the data migration process and ensure that data is correctly formatted and stored in a relational database for further analysis and utilization.

## Project Details

### 1. **Data Extraction**

- **Source**: Amazon S3
- **Data Format**: NoSQL JSON
- **Description**: The project retrieves JSON data from a specified S3 bucket. The data is in NoSQL format, which may include nested structures and various data types.

### 2. **Data Transformation**

- **Requirements**: 
  - Transform JSON data to match the required schema.
  - Ensure data consistency and integrity.
  - Apply any necessary data cleansing or enrichment operations.
- **Description**: The transformation process involves converting NoSQL JSON data into a format suitable for relational databases. This may include flattening nested structures, converting data types, and ensuring that data adheres to the target schema.

### 3. **Data Loading**

- **Destination**: Amazon RDS
- **Description**: The transformed data is loaded into an Amazon RDS instance. This allows for efficient querying, reporting, and analysis using relational database capabilities.

## Getting Started

### Prerequisites

- **AWS Account**: Access to Amazon S3 and Amazon RDS.
- **Tools and Libraries**: 
  - Python (or any other language used in the project)
  - AWS SDK (boto3 for Python)
  - Database drivers and connectors

### Setup

1. **Clone the Repository**:
   ```sh
   git clone https://github.com/ZakriyaAhmad/Raw_Data_Migration.git
