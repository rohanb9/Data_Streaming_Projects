## Project Overview
In this project, you will be provided with a real-world dataset, extracted from Kaggle, on San Francisco crime incidents, and you will provide statistical analyses of the data using Apache Spark Structured Streaming. You will draw on the skills and knowledge you've learned in this course to create a Kafka server to produce data, and ingest data through Spark Structured Streaming.

#### Development Environment
You may choose to create your project in the workspace we provide here, or if you wish to develop your project locally, you will need to set up your environment properly as described below:

* Spark 2.4.3
* Scala 2.11.x
* Java 1.8.x
* Kafka build with Scala 2.11.x
* Python 3.6.x or 3.7.x

### Output Screenshot:

#### kafka-consumer-console output:

![alt text](https://github.com/rohanb9/Data_Streaming_Projects/blob/master/SF_Crime_Statistics/snapshots/1_1_consume_data.jpg)
![alt text](https://github.com/rohanb9/Data_Streaming_Projects/blob/master/SF_Crime_Statistics/snapshots/1_Consumer_data.jpg)

#### Spark streaming (Aggregation by crime type) output:

![alt text](https://github.com/rohanb9/Data_Streaming_Projects/blob/master/SF_Crime_Statistics/snapshots/Aggregation_by_cimetype.jpg)

#### Progress Report output:

![alt text](https://github.com/rohanb9/Data_Streaming_Projects/blob/master/SF_Crime_Statistics/snapshots/queryplan.jpg)

#### QnA

Q1] How did changing values on the SparkSession property parameters affect the throughput and latency of the data?

We can monitor processedRowsPerSecond from the Progress Report.
By changing SparkSession property parameters, it shows changes in processedRowsPerSecond. It's got increased or decreased based on parameters.
It is basically the number of rows processed per second. Higher the number which means higher throughput.

Q2] What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?

->Most efficient SparkSession property:
	
  1) spark.sql.shuffle.partitions:
		spark.sql.shuffle.partitions configure the number of partitions that are used when shuffling data for joins or aggregations.
		To change the settings we can do it as:
			sqlContext.setConf("spark.sql.shuffle.partitions", "300")
  2) spark.default.parallelism:
		spark.default.parallelism is the default number of partitions in RDDs returned by transformations like join, reduceByKey, and parallelize 
    when not set explicitly by the user.spark.default.parallelism seems to only be working for raw RDD and is ignored when working with dataframes.
		To change the settings we can do it as:
			sqlContext.setConf("spark.default.parallelism", "300")
  3) spark.streaming.kafka.maxRatePerPartition:
		For rate-limiting, we can use the Spark configuration variable spark.streaming.kafka.maxRatePerPartition to set the maximum 
    number of messages per partition per batch.
		it prevents from being overwhelmed when there is a large number of unprocessed messages.

To increase parallelism, we need to increase the number of partitions in Kafka topic also we need to increase the number of consumers in the consumer group. If we increase both of them we can process topic data faster.
Also, We need to consider the number of cores and machines currently used for running jobs while increasing parallelism.

We can check the Progress Report for our job. Some of the important params are:
numInputRows : The aggregate (across all sources) number of records processed in a trigger.
inputRowsPerSecond : The aggregate (across all sources) rate of data arriving.
processedRowsPerSecond : The aggregate (across all sources) rate at which Spark is processing data.
