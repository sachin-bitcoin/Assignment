package com.todelete.experimental

import org.apache.spark.sql.SparkSession

object AssignmentScalaSpark {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("AssignmentScalaSpark").master("local[*]").getOrCreate()
    val df = spark.read.option("header","true").csv("Data_1.csv")

    import spark.implicits._
    df.filter("Action=='Purchase'").groupBy("ProductID").count().orderBy($"count".desc).show(10)
  }
}
