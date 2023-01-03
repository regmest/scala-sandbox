import org.apache.spark.sql.SparkSession
import scopt.OptionParser

object JobName {
  def main(args: Array[String]): Unit = {

    implicit val config: Configuration.Config = Configuration.optionParser.parse(args, Configuration.Config()).get

    implicit val spark: SparkSession = SparkSession.builder
      .master("local[1]")
      .getOrCreate()

    println(
      s"""Job configs:
         |config.jobName = ${config.jobName}
         """) // config.jobName = JobName$Configuration$.debug
  }

  private object Configuration {

    val jobClassName: String = this.getClass.getName

    println(s"jobClassName = $jobClassName")  // jobClassName = JobName$Configuration$

    case class Config(
                       jobName: String = jobClassName + ".debug"
                     )

    val optionParser: OptionParser[Config] = new OptionParser[Config](jobClassName) {
      arg[String]("jobName")
        .optional()
        .action { (jn, config) =>
          config.copy(jobName = jn)
        }
        .text("Job name")
    }

  }
}
