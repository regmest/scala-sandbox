import org.apache.spark.sql.types.{BooleanType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object Experiments {

  def main(args: Array[String]): Unit = {
//    val spark =getSpark()
//
//    func1(spark)
//
//    spark.close()

//    func2()
    val spark = getSpark()
    func3(spark)

  }


  def getSpark(): SparkSession = {
    SparkSession
    .builder()
    .master("local[1]")
    .getOrCreate()
  }

  def func1(spark: SparkSession) {
    val data = Seq(
      Row("a", 1, true),
      Row("a", null, null),
      Row("b", 3, false),
      Row("b", 4, false)
    )
    val schema = List(
      StructField(name = "letter", dataType = StringType, nullable = true),
      StructField(name = "num", dataType = IntegerType, nullable = true),
      StructField(name = "bool", dataType = BooleanType, nullable = true)
    )

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      StructType(schema)
    )
    df.createOrReplaceTempView("df")

    println("first_value(num)")
    spark.sql("select letter, num, first_value(num) over (partition by letter order by num) first_value from df").show()
//  +------+----+-----------+
//  |letter| num|first_value|
//  +------+----+-----------+
//  |     b|   3|          3|
//  |     b|   4|          3|
//  |     a|null|       null|
//  |     a|   1|       null|
//  +------+----+-----------+
    println("first_value(num, true)")
    spark.sql(
      """
        |select letter,
        |       num,
        |       first_value(num, true) over (partition by letter order by num) as first_value
        |       --first_value(num) IGNORE NULLS over (partition by letter order by num) as first_value -- не работает для 2.4.0
        |from df
        |""".stripMargin).show()
//      +------+----+-----------+
//      |letter| num|first_value|
//      +------+----+-----------+
//      |     b|   3|          3|
//      |     b|   4|          3|
//      |     a|null|       null|
//      |     a|   1|          1|
//      +------+----+-----------+
    println("row_number()")
    spark.sql("select letter, num, row_number() over (partition by letter order by num) as row_num from df").show()
//      +------+----+-------+
//      |letter| num|row_num|
//      +------+----+-------+
//      |     b|   3|      1|
//      |     b|   4|      2|
//      |     a|null|      1|
//      |     a|   1|      2|
//      +------+----+-------+
//    println("row_number(true)")  // так не работает
//    spark.sql("select letter, num, row_number(true) over (partition by letter order by num) as row_num from df").show()
    println("min() * 100")
    spark.sql("select letter, min(case when num is not null then num end) * 100 as num_percent from df group by 1").show()
//      +------+-----------+
//      |letter|num_percent|
//      +------+-----------+
//      |     b|        300|
//      |     a|        100|
//      +------+-----------+
    spark.sql("select *, coalesce(num,0), coalesce(bool, false) from df").show()
//      +------+----+-----+----------------+---------------------+
//      |letter| num| bool|coalesce(num, 0)|coalesce(bool, false)|
//      +------+----+-----+----------------+---------------------+
//      |     a|   1| true|               1|                 true|
//      |     a|null| null|               0|                false|
//      |     b|   3|false|               3|                false|
//      |     b|   4|false|               4|                false|
//      +------+----+-----+----------------+---------------------+
  }

  def func2(): Unit = {

    case class HotcoreSession(
                               partner: Option[String],
                               source: String
                             )

    val v1 = HotcoreSession(
      Some("value"),
      "source"
    )

    val v2 = HotcoreSession(
      None,
      "source2"
    )

    val list = v1 :: v2 :: Nil

    list.foreach(item => item.partner match {
      case Some(lksv) => println("partner found: " + lksv)
      case None => println("partner not found")
    })
  }

  def func3(spark: SparkSession): Unit = {
//     spark.sql("select regexp_extract('[#dch_closed, #dch_sth, brand=ratehawk, brand=sth]', 'brand=(\\w+)', 1)").show()
    spark.sql(
      s"""
        |with _ as (
        |select
        |     '[#dch_closed, #dch_sth, brand=ratehawk, brand=sth, $$seo]' as source_tag_str
        |)
        |select
        |      source_tag_str,
        |      regexp_extract(source_tag_str, 'brand=(\\\\w+)', 1) as source_tag_brand,
        |      regexp_extract(source_tag_str, '#dch_(\\\\w+)', 1) as source_tag_context_source,
        |       case
        |              when source_tag_str like '%cpc-meta%' then 'cpc-meta'
        |              when source_tag_str like '%cpa-meta%' then 'cpa-meta'
        |              when source_tag_str like '%#b2b-api%' then 'b2b-api'
        |              when source_tag_str like '%#b2b%' then 'b2b'
        |              when source_tag_str like '%#whitelabel%' then 'whitelabel'
        |              when source_tag_str like '%#partner%' then 'affiliate'
        |              when source_tag_str like '%$$seo%' then 'seo'
        |              when source_tag_str like '%$$sem%' then 'sem'
        |              else 'brand'
        |        end as source_tag_channel
        |from _
        |      """.stripMargin).show(truncate=false)
  }

}
