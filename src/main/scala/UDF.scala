import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// Caveats of Using Spark UDFs:
// 1. no optimizations: UDF is a Black box to Spark’s optimizer - we lose optimizations.
// (например, если наша задача прочесть данные, а затем их отфильтровать, то без UDF Spark оптимизирует и отфильтрует записи еще во время чтения,
// а с UDF сначала прочтет все, затем выполнит UDF фильтрации)
// 2. nulls: It is the responsibility of the programmer to handle nulls (если в обрабатываемой колонке будет null, и такой случай не будет обработан в UDF, будет ошибка


object UDF {

  def main(args: Array[String]): Unit = {

    implicit val spark: SparkSession = SparkSession
      .builder()
      .master("local[1]")
      .getOrCreate()

    val userData = spark.createDataFrame(Seq(
      (1, "Chandler", "Pasadena", "US"),
      (2, "Monica", "New york", "USa"),
      (3, "Phoebe", "Suny", "USA"),
      (4, "Rachael", "St louis", "United states of America"),
      (5, "Joey", "LA", "Ussaa"),
      (6, "Ross", "Detroit", "United states")
    )).toDF("id", "name", "city", "country")

    val normaliseCountry = spark.udf.register("normalisedCountry", cleanCountry) // UDF register
    userData.withColumn("normalisedCountry", normaliseCountry(col("country"))).show // Now we can use normaliseCountry scala function as if it’s spark SQL functions.
//    +---+--------+--------+--------------------+-----------------+
//    | id|    name|    city|             country|normalisedCountry|
//    +---+--------+--------+--------------------+-----------------+
//    |  1|Chandler|Pasadena|                  US|              USA|
//    |  2|  Monica|New york|                 USa|              USA|
//    |  3|  Phoebe|    Suny|                 USA|              USA|
//    |  4| Rachael|St louis|United states of ...|              USA|
//    |  5|    Joey|      LA|               Ussaa|          unknown|
//    |  6|    Ross| Detroit|       United states|              USA|
//    +---+--------+--------+--------------------+-----------------+

    // for Spark SQL
    userData.createOrReplaceTempView("user_data")
    spark.udf.register("cleanCountry", cleanCountry)
    spark.sql("select * ,cleanCountry(country) as normalisedCountry from  user_data")
    spark.sql("select * ,cleanCountry(country) as normalisedCountry from  user_data").show
    // same

    // registering with udf()
    import org.apache.spark.sql.functions.{col, udf}
    val cleanCountryUdf = udf(cleanCountry)
    userData.withColumn("normalisedCountry", cleanCountryUdf(col("country"))).show

    // to show all UDFs
    spark.sql("SHOW USER FUNCTIONS").collect

  }

  def cleanCountry: String => String = (country: String) => {
    val allUSA = Seq("US", "USa", "USA", "United states", "United states of America") // All possible combinations of US that could be misspelled
    if (allUSA.contains(country)) {
      "USA"
    }
    else {
      "unknown"
    }

  }
}
