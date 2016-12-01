## Prerequisites
0. JDK 1.8+
1. Scala SDK 2.11.8+
2. Spark 2.0.1 loaded in .mvn
3. SBT  0.13.13
4. SBT assembly -> [get it here on github](https://github.com/sbt/sbt-assembly)

## Building jar
Run from `ph3/scala_code`
```shell
   sbt assembly 
```
## Spark-Submitting jar
```shell
  spark-submit --class io.purush.spark.giscup.script.CellScript target/scala-2.11/scala_code-assembly-0.1.jar 
```
