import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import com.github.vdao.example.utils.SparkSessionTestWrapper
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatest.FunSpec
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ApplicationSpec extends FunSpec
  with DataFrameComparer
  with SparkSessionTestWrapper {

  import spark.implicits._

  it("appends a columt to a DataFrame") {

    val sourceDf = Seq(
      ("a"),
      ("b")
    ).toDF("col1")

    val actualDf = sourceDf.transform(Application.withGreeting())

    val expectedSchema = List(
      StructField("col1", StringType, true),
      StructField("greeting", StringType, false)
    )

    val expectedData = Seq(
      Row("a", "hello world"),
      Row("b", "hello world")
    )

    val expectedDf = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(expectedSchema)
    )

    assertSmallDataFrameEquality(actualDf, expectedDf)
  }
}
