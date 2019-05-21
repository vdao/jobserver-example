package com.github.vdao.example

import org.apache.spark.sql.{DataFrame, SparkSession}

class Parser(spark: SparkSession) {

  def parse(resource: String): DataFrame =
    spark.read.format("json").json(resource)
}
