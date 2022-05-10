
object SparkHDFS extends App {

  import org.apache.spark.sql.SparkSession

  val path = if (args.length > 0) args(0)
  else "hdfs://localhost:9000/files/"

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  val input = spark
    .read
    .text(path)
    .show(truncate = false)
}
