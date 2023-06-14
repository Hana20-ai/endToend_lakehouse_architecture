import org.apache.spark.sql.{SaveMode, SparkSession}
//this script converts parquet files in the input dir to delta files in the outpu dir. delta tables have the same name as the parquet files 

object ConvertParquetToDeltaScript {
  def main(args: Array[String]): Unit = {
    val inputPath = args(0)
    val outputPath = args(1)

    convertParquetToDelta(inputPath, outputPath)
  }



  def convertParquetToDelta(inputPath: String, outputPath: String): Unit = {
    val spark = SparkSession.builder()
      .appName("ConvertParquetToDelta")
      .getOrCreate()

    val fileList = spark.read.format("parquet").load(inputPath).inputFiles

    fileList.foreach { file =>
      val df = spark.read.format("parquet").load(file)
      val fileName = file.split("/").last
      val deltaPath = s"$outputPath/$fileName"
      df.write.format("delta").mode(SaveMode.Append).save(deltaPath)
    }

    //spark.stop()
  }
}
