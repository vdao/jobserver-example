package com.github.vdao.example

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import com.github.vdao.example.utils.SparkSessionTestWrapper
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.junit.runner.RunWith
import org.scalatest.FunSpec
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ParserSpec extends FunSpec
  with DataFrameComparer
  with SparkSessionTestWrapper {

  it("should read test data into DataFrame") {
    val expectedSchema = List(
      StructField("event_type", StringType, true),
      StructField("id", StringType, true),
      StructField("impression_id", StringType, true),
      StructField("movie", LongType, true),
      StructField("movies", ArrayType(LongType), true),
      StructField("user_id", LongType, true)
    )

    val expectedData = Seq(
      Row("impression", "67d38186-bb30-463d-82e6-ee636dfa7c57", null, null, Seq(1L, 2L, 3L, 4L, 5L, 6L), 1L),
      Row("impression", "a759596a-ae4b-4610-ae4b-c97d2212eba5", null, null, Seq(1L, 2L, 3L, 4L, 5L, 6L), 2L),
      Row("impression", "46b4d266-eb19-4d9b-bdee-14a32afaba1c", null, null, Seq(1L, 2L, 3L, 4L, 5L, 6L), 3L),
      Row("impression", "5ada269e-d1ef-47b9-b7a6-eca4d3156273", null, null, Seq(1L, 2L, 3L, 4L, 5L, 6L), 4L),
      Row("impression", "3a8818a1-947f-48e9-ac8e-b3217919adca", null, null, Seq(1L, 2L, 3L, 4L, 5L, 6L), 5L),
      Row("impression", "fae7d5ff-118a-4e3c-8302-1a04f1aff8ce", null, null, Seq(1L, 2L, 3L, 4L, 5L, 6L), 6L),
      Row("impression", "30f5b38e-ff00-4eba-8955-73d1dfb75259", null, null, Seq(1L, 2L, 3L, 4L, 5L, 6L), 7L),
      Row("impression", "bad942ba-991b-4deb-bcfe-d63d15357701", null, null, Seq(1L, 2L, 3L, 4L, 5L, 6L), 8L),
      Row("impression", "c60cca7a-ba23-4b83-9c37-d597fdf07e30", null, null, Seq(1L, 2L, 3L, 4L, 5L, 6L), 9L),
      Row("impression", "2e45d4e8-79ba-4e2e-ae6e-6aed3f787532", null, null, Seq(1L, 2L, 3L, 4L, 5L, 6L), 10L),
      Row("impression", "d955d25c-00f6-4975-9e39-4cf65ec8cd97", null, null, Seq(1L, 2L, 3L, 4L, 5L, 6L), 11L),
      Row("impression", "91095659-73c6-4d92-90dc-34f70cf820f5", null, null, Seq(1L, 2L, 3L, 4L, 5L, 6L), 12L),
      Row("impression", "c6dd60aa-5f2f-4977-b650-70f7455f4698", null, null, Seq(1L, 2L, 3L, 4L, 5L, 6L), 13L),
      Row("impression", "f8e9abf5-23b4-43ed-a598-bf2c610da6de", null, null, Seq(1L, 2L, 3L, 4L, 5L, 6L), 14L),
      Row("impression", "2f91fd16-6318-4d88-a99c-9749bf3d9de9", null, null, Seq(1L, 2L, 3L, 4L, 5L, 6L), 15L),
      Row("impression", "cd1f2e9f-8e2b-42ff-911b-05da2ee52c51", null, null, Seq(1L, 2L, 3L, 4L, 5L, 6L), 16L),
      Row("conversion", null, "3a8818a1-947f-48e9-ac8e-b3217919adca", 1L, null, null),
      Row("conversion", null, "3a8818a1-947f-48e9-ac8e-b3217919adca", 2L, null, null),
      Row("conversion", null, "3a8818a1-947f-48e9-ac8e-b3217919adca", 3L, null, null),
      Row("conversion", null, "c6dd60aa-5f2f-4977-b650-70f7455f4698", 1L, null, null),
      Row("conversion", null, "c6dd60aa-5f2f-4977-b650-70f7455f4698", 2L, null, null),
      Row("conversion", null, "c6dd60aa-5f2f-4977-b650-70f7455f4698", 4L, null, null),
      Row("conversion", null, "f8e9abf5-23b4-43ed-a598-bf2c610da6de", 3L, null, null),
      Row("conversion", null, "f8e9abf5-23b4-43ed-a598-bf2c610da6de", 4L, null, null),
      Row("conversion", null, "f8e9abf5-23b4-43ed-a598-bf2c610da6de", 6L, null, null),
      Row("conversion", null, "bad942ba-991b-4deb-bcfe-d63d15357701", 4L, null, null),
      Row("conversion", null, "bad942ba-991b-4deb-bcfe-d63d15357701", 5L, null, null),
      Row("conversion", null, "bad942ba-991b-4deb-bcfe-d63d15357701", 6L, null, null),
    )

    val expectedDf = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(expectedSchema)
    )

    val actualDf = new Parser(spark).parse("test-data/events.txt")

    assertSmallDataFrameEquality(actualDf, expectedDf)
  }

}
