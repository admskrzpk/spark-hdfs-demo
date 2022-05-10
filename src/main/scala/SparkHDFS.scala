import org.apache.spark.sql.SparkSession

object SparkHDFS {
  def main(args: Array[String]) = {
    import org.apache.spark.sql.SparkSession

    val path = if (args.length > 0) args(0)
    else "hdfs://localhost:9000/files/"

    println(s"loading files from: $path")

   implicit val spark = SparkSession
      .builder()
      .getOrCreate()
    val output = loadFilesFromPath(path)
    output.show(truncate = false)
  }

  def loadFilesFromPath(path: String) (implicit spark: SparkSession) = {
    spark
      .read
      .text(path)
  }
}