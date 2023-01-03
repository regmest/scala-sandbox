import Experiments.func1
import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime

import java.nio.file.{Path, Paths}

object ScalaExperiments {

  def main(args: Array[String]): Unit = {


    val spark = SparkSession
      .builder()
      .master("local[1]")
      .getOrCreate()

    func1(spark)

    spark.close()
  }

  def beginningOfDay(dt: DateTime): DateTime = dt.withTime(0, 0, 0, 0)
  def beginningOfHour(dt: DateTime): DateTime = dt.withTime(dt.getHourOfDay, 0, 0, 0)

  def func1(spark: SparkSession): Unit = {

    val outputPath: String = "/data/tmp/hotcore_hotel_availability"
    val executionDate: DateTime = DateTime.now()
//    val executionHour: DateTime = beginningOfHour(DateTime.now())
    val fullOutputPath: String = outputPath +
      (if (outputPath.endsWith("/")) "" else "/") +
      s"date=${executionDate.toString("yyyy-MM-dd")}/hour=${executionDate.getHourOfDay}"
//      s"date=${executionDate.toString("yyyy-MM-dd")}/hour=${executionHour.getHourOfDay}"

    println(s" -- fullOutputPath = $fullOutputPath -- ")
    // -- fullOutputPath = /data/tmp/hotcore_hotel_availability/date=2022-12-02/hour=17 --

    println(s"beginningOfHour(DateTime.now()) = ${beginningOfHour(DateTime.now())}")
    println(s"beginningOfDay(DateTime.now()) = ${beginningOfDay(DateTime.now())}")
    //beginningOfHour(DateTime.now()) = 2022-12-02T17:00:00.000+03:00
    //beginningOfDay(DateTime.now()) = 2022-12-02T00:00:00.000+03:00

  }

}
