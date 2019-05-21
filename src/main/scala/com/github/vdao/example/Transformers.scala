package com.github.vdao.example

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, explode_outer}

object Transformers {

  def withExplodedImpressions()(source: DataFrame): DataFrame =
    source
      .withColumn("movie_impression", explode_outer(col("movies")))
      .drop("movies")
}
