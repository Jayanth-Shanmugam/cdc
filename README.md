# PostgreSQL -> Apache Iceberg Change Data Capture (CDC) Pipeline
A pipeline to capture changes from a postgres database and stream them to an iceberg table using Apache Kafka. CDC pipelines are useful when we want to propagate data changes between systems in real-time or near real-time. For instance, if we want to real-time analytics on our data lakes, synchronizing data from the source to our data lake using traditional batch updates might take more time than expected and CDC pipelines offer a solution to this.

# Architecture
