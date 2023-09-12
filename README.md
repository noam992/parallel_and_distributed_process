# Parallel and Distributed Process
When we choose to work in a distributed and parallel manner, what happens, and does the entire process actually become shorter?

## Overture
When working with big data, it is essential to ensure that the processes are as efficient and fast as possible.
PySpark provides a syntax for distributed processing, but sometimes not all libraries can work with Pyspark.
I'm sharing the tests I conducted for distributed execution, where I directly used the computer's processor and worked in parallel with its workers.

## Result
Reading and printing 1000 records from a CSV file with a wait time of 0.1 seconds will take about 1.5 minutes. The use is with one processor as a thread process.
Based on the tests and the attached script, you can see the results of the same read using distributed and parallel work using the computer's workers:
1. The latency is slightly slower, meaning the time for the process to start until the start of reading the records.
2. Requires additional functionality that defines the distributed method, according to which the run will divide the records of the file between the processors. In this test, I set it according to the number of lines in the file, but it can also be defined by a date column, etc.
3. The total run time was significantly reduced and almost divided itself by the number of processors on which it works.

## Attachments
* The CSV file was taken from the KAGGLE data repository named Airline Dataset, which was published by SOURAV BANERJEE.
* It is important to check how many processors the computer on which you are running the script contains.
