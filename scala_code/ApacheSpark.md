# Apache Spark

## Architecture

Spark applications are run from the Spark driver program. The Spark driver is any program that has an associated SparkContext object. Once the spark process is started on a standalone machine or on the master in a cluster, the driver program can obtain the SparkContext in a compiled Scala program by using:

```scala
val sc = new SparkContext(master="spark://master:7077")
```

or by running a spark shell through:

```shell
$ spark-shell --master spark://master:7077
```

Each machine in a Spark cluster have Executors associated with. Executors are JVM threads that are designed to operate on the RDDs associated with that node in the cluster.

The Spark driver program with the SparkContext is responsible for creation, execution of jobs by utilizing the DAGScheduler and Task Schedulers, which are responsible for spinning threads on worker nodes to execute the Spark Job.

## RDDs

RDDs or Resilient Distributed Datasets are the fundamental unit of data in the Spark environment. RDDs are built on top of the Scala collections framework[1] and they extend on them by adding distributed properties and concurrency to them.

RDDs are :

- Resilient — RDDs manage a lineage graph related to the transformations and actions performed on them. This makes them fault tolerant and ability to recover from failures. Also RDDs can be replicated on nodes to increase availability and increase fault tolerance and reliability.
- Distributed — RDDs are distributed across nodes, i.e partitioned across nodes in a cluster.
- Dataset — extended on scala collections that represent lists, arrays and maps of primitive types and serializable objects.

In addition RDDs are in-memory partitioned data structures. They exist solely in memory, spilling to disk optionally when memory is limited on the node.

RDDs are lazily evaluated and are cacheable. Spark's speed is due to the in memory property and the lazy evaluations of RDDs.

RDDs are partitioned in memory, they exist across the cluster residing one per JVM. In addition, RDDs can use the native Distributed filesystems like HDFS and utilize their partitioning and replication frameworks.

## Operations on RDDs, DAGs, transformations and actions

RDDs have two fundamental operations:

​	— Transformations and,

​	— Actions

Transformations are operations that are evaluated lazily. Transformations convert the RDD from one form to another. For each transformation on a RDD, a new RDD object is created, and they are linked through a DAG as shown below:

<DIAG1>

Any RDD stores a list of properties:

— List of parent RDDs from which the current RDD is generated from by using transformations

— Partition indices

— A function object that contains the operation to be done on the partitions

Actions are operations that trigger all operations above it(parent RDDs and computations) to finally execute. Actions are those operations that kick the lazy transformations into life and execute the whole DAG of tasks associated with a Spark job.

Examples of transformation are - `map`, `filter`, `flatten`, `flatMap`, `flatMapToPair`, `reduceByKey`, `join`, `cogroup` etc.

Examples of actions are `count`, `collect`,`saveAsTextFile`.

## DAGs

All spark operations are pushed into a Directed Acyclic Graph of tasks. The scheduler at the Driver program is responsible for this.

Example DAG - 

<DIAG2>

When an action is triggered the graph is traversed in reverse from the bottom-most RDD by seeking parents and finally executing them down the DAG till the action that triggered the evaluation of lazy transformation above it.

# Note on Spark Performance

## Stages

When an action is called on an RDD, a job is created for all operations till the action. The job is then transformed into stages submitted to the TaskScheduler for execution.

Stages are individual groups of operations that can be pipelined. For example, in our program, we have a bunch of maps and filters applied on our initial Hadoop data RDD(created using `sc.textFile("hdfs://master:54310/input")`). Then a `reduceByKey` is called on top of these operations. Further maps and filters are applied to get the Getis-ord statistic. The action that invokes the overall job is the `takeOrdered(50)` operation.

<DIAG3>

<DIAG4>

Here we see that the stages are split at the `reduceByKey` operation. The reason is that Spark splits stages based on shuffle operations. Each operation prior to the shuffle on `reduceByKey` operation can be pipelined across the RDD.

However the `reduceByKey` is a shuffle operation and hence has to be staged separately from the other operations.

## Shuffles — Costliest operations

Shuffles are the costliest operations on Spark. Like hadoop-mapreduce, the shuffle stage involves sorting of data in some sort and this involves moving data across the network, involving high transfer costs. Also sorting is a `O(nlogn)` operation while maps, filters, reduce, are `O(n)` operations.

Hence to optimize a Spark job, one has to write operations in a way that reduces the number of shuffle operations as much as possible.

## Other optimizations — serialization of objects

Another optimization that can be done with Spark is to reduce the overall memory footprint of objects and RDDs in the JVM. The idea is to use primitive types, Scala collections as much as possible as they are by default serialized by Spark and are optimized in RDD implementations[2]. In our application, we have reduced significant memory footprint compared to those implementations with GeoSpark by using Tuples of Integers and Longs for all our data-structures involving cells. Reducing memory footprint is important to avoid Spark from spilling into disk and hence incurring disk I/O costs.



# Spark UI

The Spark UI on `localhost:8080` is a useful tool to monitor the cluster spark resources, see scheduled jobs, see execution times and job failures.

<SPARKUI>

We primarily leveraged the Spark UI to understand the execution framework, DAGs and Stages of RDDs to optimize on reducing the shuffle stage.

# Ganglia monitoring 

<Screenshots>

## Notes on memory usage:

Compare VM vs normal — more vs less memory

<memory>

We see here that the memory usage is high initially and then there is a dip in the memory. This is due to the fact that the initially operations are performed on the point RDD which is a 1.9GB distributed RDD. Once we reduce the data by key and have cell RDD we see that the memory usage dips. This also correlates well with the network peak due to shuffle @t=20:15  in the network graph.

<network>

## Notes on CPU usage

<CPU>

It is seen that there is a spike in CPU usage right after t=20:15. This is due to the computationally intensive Getis-Ord function that computes with high-precision Double numbers.

## Notes on network usage

<Network>

The network usage peaks are due to the `reduceByKey` task. Since our application uses only one `reduceByKey` shuffle stage, there is the one peak in network activity during the whole execution.





# References

[1] Zaharia, Matei, et al. "Resilient distributed datasets: A fault-tolerant abstraction for in-memory cluster computing." *Proceedings of the 9th USENIX conference on Networked Systems Design and Implementation*. USENIX Association, 2012.

[2]https://spark.apache.org/docs/latest/tuning.html#data-serialization