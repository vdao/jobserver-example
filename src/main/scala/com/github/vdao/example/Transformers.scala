package com.github.vdao.example

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, explode_outer}

object Transformers {

  def withExplodedImpressions()(source: DataFrame): DataFrame =
    source
      .withColumn("movie_impression", explode_outer(col("movies")))
      .drop("movies")

  def withJoinedConversions()(source: DataFrame): DataFrame = {
    // TODO assert on schema

    val conversions = source.filter(col("event_type") === "conversion")
      .select(
        col("conversion_id"),
        col("impression_id"),
        col("movie").as("movie_impression"))

    val impressions = source.filter(col("event_type") === "impression")

    impressions.join(conversions,
      (impressions("impression_id") <=> conversions("impression_id"))
        && (impressions("movie_impression") <=> conversions("movie_impression")),
      "leftouter")
  }
}
