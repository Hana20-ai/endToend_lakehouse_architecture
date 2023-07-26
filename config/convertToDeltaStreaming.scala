import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

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

    val inputStream = spark.readStream
      .format("parquet")
      .load(inputPath)

    val query = inputStream.writeStream
      .foreachBatch { (batchDF, batchId) =>
        val fileList = batchDF.inputFiles

        batchDF.foreach { row =>
          val file = row.getString(0)
          val df = spark.read.format("parquet").load(file)
          val fileName = file.split("/").last
          val deltaPath = s"$outputPath/$fileName"
          df.write.format("delta").mode(SaveMode.Append).save(deltaPath)
        }
      }
      .outputMode(OutputMode.Append())
      .trigger(Trigger.ProcessingTime(0))
      .start()

    query.awaitTermination()
  }
}