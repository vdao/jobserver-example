package com.github.vdao.example

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import com.github.vdao.example.utils.SparkSessionTestWrapper
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.FunSpec

class TransformersSpec extends FunSpec
  with DataFrameComparer
  with SparkSessionTestWrapper {

  it("should append exploded impressions") {
    val sourceSchema = List(
      StructField("event_type", StringType, true),
      StructField("movies", ArrayType(LongType), true),
    )

    val sourceData = Seq(
      Row("impression", Seq(1L, 2L, 3L, 4L, 5L, 6L)),
      Row("conversion", null),
    )

    val sourceDf = spark.createDataFrame(
      spark.sparkContext.parallelize(sourceData),
      StructType(sourceSchema)
    )

    val expectedSchema = List(
      StructField("event_type", StringType, true),
      StructField("movie_impression", LongType, true)
    )

    val expectedData = Seq(
      Row("impression", 1L),
      Row("impression", 2L),
      Row("impression", 3L),
      Row("impression", 4L),
      Row("impression", 5L),
      Row("impression", 6L),
      Row("conversion", null),
    )

    val expectedDf = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(expectedSchema)
    )

    val actualDf = sourceDf.transform(Transformers.withExplodedImpressions())

    assertSmallDataFrameEquality(actualDf, expectedDf)
  }

  it("should join conversions to impressions and remove conversions from DF") {
    val sourceSchema = List(
      StructField("conversion_id", StringType, true),
      StructField("event_type", StringType, true),
      StructField("impression_id", StringType, true),
      StructField("movie", LongType, true),
      StructField("user_id", LongType, true),
      StructField("movie_impression", LongType, true)
    )

    val sourceData = Seq(
      Row(null, "impression", "67d38186-bb30-463d-82e6-ee636dfa7c57", null, 1L, 1L),
      Row(null, "impression", "67d38186-bb30-463d-82e6-ee636dfa7c57", null, 1L, 2L),
      Row(null, "impression", "67d38186-bb30-463d-82e6-ee636dfa7c57", null, 1L, 3L),
      Row(null, "impression", "67d38186-bb30-463d-82e6-ee636dfa7c57", null, 1L, 4L),
      Row(null, "impression", "67d38186-bb30-463d-82e6-ee636dfa7c57", null, 1L, 5L),
      Row(null, "impression", "67d38186-bb30-463d-82e6-ee636dfa7c57", null, 1L, 6L),
      Row("d42d75c6-07ca-4d5c-bf71-89e08443c023", "conversion", "67d38186-bb30-463d-82e6-ee636dfa7c57", 1L, null, null),
    )

    val sourceDf = spark.createDataFrame(
      spark.sparkContext.parallelize(sourceData),
      StructType(sourceSchema)
    )

    val expectedSchema = List(
      StructField("conversion_id", StringType, true),
      StructField("event_type", StringType, true),
      StructField("impression_id", StringType, true),
      StructField("movie", LongType, true),
      StructField("user_id", LongType, true),
      StructField("movie_impression", LongType, true),
      StructField("conversion_id", StringType, true),
      StructField("impression_id", StringType, true),
      StructField("movie_impression", LongType, true)
    )

    val expectedData = Seq(
      Row(null, "impression", "67d38186-bb30-463d-82e6-ee636dfa7c57", null, 1L, 2L, null, null, null),
      Row(null, "impression", "67d38186-bb30-463d-82e6-ee636dfa7c57", null, 1L, 6L, null, null, null),
      Row(null, "impression", "67d38186-bb30-463d-82e6-ee636dfa7c57", null, 1L, 5L, null, null, null),
      Row(null, "impression", "67d38186-bb30-463d-82e6-ee636dfa7c57", null, 1L, 3L, null, null, null),
      Row(null, "impression", "67d38186-bb30-463d-82e6-ee636dfa7c57", null, 1L, 1L, "d42d75c6-07ca-4d5c-bf71-89e08443c023", "67d38186-bb30-463d-82e6-ee636dfa7c57", 1L),
      Row(null, "impression", "67d38186-bb30-463d-82e6-ee636dfa7c57", null, 1L, 4L, null, null, null),
    )

    val expectedDf = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(expectedSchema)
    )

    val actualDf = sourceDf.transform(Transformers.withJoinedConversions())

    assertSmallDataFrameEquality(actualDf, expectedDf)
  }
}
