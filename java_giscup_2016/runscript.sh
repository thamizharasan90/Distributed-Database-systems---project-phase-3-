#!/bin/zsh
mvn clean && mvn compile && mvn package
spark-submit --master spark://pswaminathan-23730:7077 --class io.purush.spark.giscup.java.solution.YellowTaxiJob target/java_giscup_2016-1.0-SNAPSHOT.jar hdfs://localhost:54310/inputBig ./results.csv
