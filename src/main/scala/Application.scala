import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Application {
  def withGreeting()(df: DataFrame): DataFrame = {
    df.withColumn("greeting", lit("hello world"))
  }
}
