# SF Crime Project
## Project Overview
In this project, I used real-world dataset, extracted from Kaggle, on San Francisco crime incidents, to perform statistical analyses using Apache Spark Structured Streaming. The project is a part of the Nano Degree in Data Streaming at Udacity.
The files in the src directory in this repo constitute the python files that formed the programming part of the project. The data directory contains the data files for the project. The zip file should be unzipped first.

# Results
The image (png) files in the reports directory of this repo are screenshots that were taken while executing the python files, kafka and spark. 
# Answer to Questions
Below are the answers answers that were obtained from executing the files.
## Question 1
How did changing values on the SparkSession property parameters affect the throughput and latency of the data?
### Answer
Changing values of the SparkSession property parameters either increased or decreased the throughput metrics from the spark streaming sessions. One metric that was noticeable was processedRowsPerSecond. This metric was affected by the values of the spark session parameters.
## Question 2
What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?
### Answer
There are a combination of factors that could afftect throughput but I found the following parameters to have the most effect on the processedRowsPerSecond:
spark.sql.shuffle.partitions, spark.default.parallelism and spark.kafka.streaming.maxRatePerPartition. By changing these values from the default, I found that a 3-fold improvement could be obtained in the processedRowsPerSecond.
