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
      StructField("id", StringType, true),
      StructField("impression_id", StringType, true),
      StructField("movie", LongType, true),
      StructField("movies", ArrayType(LongType), true),
      StructField("user_id", LongType, true)
    )

    val sourceData = Seq(
      Row("impression", "67d38186-bb30-463d-82e6-ee636dfa7c57", null, null, Seq(1L, 2L, 3L, 4L, 5L, 6L), 1L),
      Row("conversion", null, "3a8818a1-947f-48e9-ac8e-b3217919adca", 1L, null, null),
    )

    val sourceDf = spark.createDataFrame(
      spark.sparkContext.parallelize(sourceData),
      StructType(sourceSchema)
    )

    val expectedSchema = List(
      StructField("event_type", StringType, true),
      StructField("id", StringType, true),
      StructField("impression_id", StringType, true),
      StructField("movie", LongType, true),
      StructField("user_id", LongType, true),
      StructField("movie_impression", LongType, true)
    )

    val expectedData = Seq(
      Row("impression", "67d38186-bb30-463d-82e6-ee636dfa7c57", null, null, 1L, 1L),
      Row("impression", "67d38186-bb30-463d-82e6-ee636dfa7c57", null, null, 1L, 2L),
      Row("impression", "67d38186-bb30-463d-82e6-ee636dfa7c57", null, null, 1L, 3L),
      Row("impression", "67d38186-bb30-463d-82e6-ee636dfa7c57", null, null, 1L, 4L),
      Row("impression", "67d38186-bb30-463d-82e6-ee636dfa7c57", null, null, 1L, 5L),
      Row("impression", "67d38186-bb30-463d-82e6-ee636dfa7c57", null, null, 1L, 6L),
      Row("conversion", null, "3a8818a1-947f-48e9-ac8e-b3217919adca", 1L, null, null),
    )

    val expectedDf = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(expectedSchema)
    )

    val actualDf = sourceDf.transform(Transformers.withExplodedImpressions())

    assertSmallDataFrameEquality(actualDf, expectedDf)
  }
}
