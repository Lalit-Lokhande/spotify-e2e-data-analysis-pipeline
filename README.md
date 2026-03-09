# spotify-e2e-data-analysis-pipeline
Spotify end to end data pipeline project.

# Project Goal
1 Data Ingestion - Ingestion of the data using Spotify API for both Public and Private playlists

2 ETL - Get the data in raw JSON format and transform the data in tabular format using Pandas API after flattening it and load it in AWS S3, query using AWS Athena

3 AWS Services - Use AWS services listed below for this End-to-End pipeline which will fetch the data for every 1 hour, transform it and provide the tabular data for further analytical purpose

# Services Used
1 AWS Lambdas - AWS managed serverless computing service that runs the code in response to events and automatically manages the compute resources.

2 AWS S3 - Amazon Simple Storage Service is an object storage service that offers industry-leading scalability, data availability, security, and performance.

3 AS EventBridge - It is a serverless service that uses events to connect application components together, making it easier to build scalable event-driven applications.

4 AWS Glue - Fully AWS managed, serverless data integration service that simplifies the process of discovering, preparing, and combining data for analytics.

5 AWS Athena - A serverless, interactive query service that enables analysis of data directly in Amazon S3 using standard SQL.


# Architecture Diagram of Project
https://github.com/Lalit-Lokhande/spotify-e2e-data-analysis-pipeline/blob/main/Architecture%20Diagram.jpg
