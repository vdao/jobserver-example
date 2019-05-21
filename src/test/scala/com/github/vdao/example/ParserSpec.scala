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
      StructField("conversion_id",StringType,true),
      StructField("event_type", StringType, true),
      StructField("impression_id", StringType, true),
      StructField("movie", LongType, true),
      StructField("movies", ArrayType(LongType), true),
      StructField("user_id", LongType, true)
    )

    val expectedData = Seq(
      Row(null, "impression", "67d38186-bb30-463d-82e6-ee636dfa7c57", null, Seq(1L, 2L, 3L, 4L, 5L, 6L), 1L),
      Row(null, "impression", "a759596a-ae4b-4610-ae4b-c97d2212eba5", null, Seq(1L, 2L, 3L, 4L, 5L, 6L), 2L),
      Row(null, "impression", "46b4d266-eb19-4d9b-bdee-14a32afaba1c", null, Seq(1L, 2L, 3L, 4L, 5L, 6L), 3L),
      Row(null, "impression", "5ada269e-d1ef-47b9-b7a6-eca4d3156273", null, Seq(1L, 2L, 3L, 4L, 5L, 6L), 4L),
      Row(null, "impression", "3a8818a1-947f-48e9-ac8e-b3217919adca", null, Seq(1L, 2L, 3L, 4L, 5L, 6L), 5L),
      Row(null, "impression", "fae7d5ff-118a-4e3c-8302-1a04f1aff8ce", null, Seq(1L, 2L, 3L, 4L, 5L, 6L), 6L),
      Row(null, "impression", "30f5b38e-ff00-4eba-8955-73d1dfb75259", null, Seq(1L, 2L, 3L, 4L, 5L, 6L), 7L),
      Row(null, "impression", "bad942ba-991b-4deb-bcfe-d63d15357701", null, Seq(1L, 2L, 3L, 4L, 5L, 6L), 8L),
      Row(null, "impression", "c60cca7a-ba23-4b83-9c37-d597fdf07e30", null, Seq(1L, 2L, 3L, 4L, 5L, 6L), 9L),
      Row(null, "impression", "2e45d4e8-79ba-4e2e-ae6e-6aed3f787532", null, Seq(1L, 2L, 3L, 4L, 5L, 6L), 10L),
      Row(null, "impression", "d955d25c-00f6-4975-9e39-4cf65ec8cd97", null, Seq(1L, 2L, 3L, 4L, 5L, 6L), 11L),
      Row(null, "impression", "91095659-73c6-4d92-90dc-34f70cf820f5", null, Seq(1L, 2L, 3L, 4L, 5L, 6L), 12L),
      Row(null, "impression", "c6dd60aa-5f2f-4977-b650-70f7455f4698", null, Seq(1L, 2L, 3L, 4L, 5L, 6L), 13L),
      Row(null, "impression", "f8e9abf5-23b4-43ed-a598-bf2c610da6de", null, Seq(1L, 2L, 3L, 4L, 5L, 6L), 14L),
      Row(null, "impression", "2f91fd16-6318-4d88-a99c-9749bf3d9de9", null, Seq(1L, 2L, 3L, 4L, 5L, 6L), 15L),
      Row(null, "impression", "cd1f2e9f-8e2b-42ff-911b-05da2ee52c51", null, Seq(1L, 2L, 3L, 4L, 5L, 6L), 16L),
      Row("ac63513a-70ea-472d-8909-e0d2c1d77ccf", "conversion", "3a8818a1-947f-48e9-ac8e-b3217919adca", 1L, null, null),
      Row("fc9cf792-9d50-4804-8c04-b6d5f072c2ae", "conversion", "3a8818a1-947f-48e9-ac8e-b3217919adca", 2L, null, null),
      Row("c74a06c5-6adb-408e-9037-2f95f8e80270", "conversion", "3a8818a1-947f-48e9-ac8e-b3217919adca", 3L, null, null),
      Row("b7ab1b5b-dadb-4e22-8eb5-b21b3f1f1b07", "conversion", "c6dd60aa-5f2f-4977-b650-70f7455f4698", 1L, null, null),
      Row("be5843cd-492b-41ee-bc1a-f20c85cb0259", "conversion", "c6dd60aa-5f2f-4977-b650-70f7455f4698", 2L, null, null),
      Row("d42d75c6-07ca-4d5c-bf71-89e08443c023", "conversion", "c6dd60aa-5f2f-4977-b650-70f7455f4698", 4L, null, null),
      Row("ac2d39d9-07c1-41d6-8775-2f8af06c0417", "conversion", "f8e9abf5-23b4-43ed-a598-bf2c610da6de", 3L, null, null),
      Row("e5318cf8-d006-4e32-a1f3-fd2acf9346cf", "conversion", "f8e9abf5-23b4-43ed-a598-bf2c610da6de", 4L, null, null),
      Row("bb4ba88f-b93f-4624-97e0-cd48ee7eaba9", "conversion", "f8e9abf5-23b4-43ed-a598-bf2c610da6de", 6L, null, null),
      Row("4d650e19-3ed2-4ea2-b4f7-5ab12a1dbcd2", "conversion", "bad942ba-991b-4deb-bcfe-d63d15357701", 4L, null, null),
      Row("184b2250-54da-44fd-99ba-6bfa1ad9ee59", "conversion", "bad942ba-991b-4deb-bcfe-d63d15357701", 5L, null, null),
      Row("53664fac-5ab0-435a-98e2-2bcd8fd41c78", "conversion", "bad942ba-991b-4deb-bcfe-d63d15357701", 6L, null, null),
    )

    val expectedDf = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(expectedSchema)
    )

    val actualDf = new Parser(spark).parse("test-data/events.txt")

    actualDf.printSchema()
    actualDf.show(100)

    assertSmallDataFrameEquality(actualDf, expectedDf)
  }

}
