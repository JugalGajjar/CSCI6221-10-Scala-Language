import sttp.client3._
import play.api.libs.json._
import java.io._
import scala.io.Source
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import java.nio.file.{Files, Paths, StandardCopyOption}
import java.io.PrintWriter
import org.apache.spark.sql.SparkSession

case class VideoDetails2(
                          videoId: String,
                          title: String,
                          viewCount: String,
                          likeCount: String,
                          commentCount: String
                        )

object YouTubeVideoFetcher2 {

  // Replace with your YouTube API key
  val apiKey: String = "AIzaSyAUjKQEzzoZBOqRSi7tLIugfxyIh9ybIZk"

  // Function to fetch video details by video ID (one by one)
  def fetchVideoDetails2(videoId: String): Option[VideoDetails2] = {
    val baseUrl = "https://www.googleapis.com/youtube/v3/videos"

    val params = Map(
      "part" -> "snippet,statistics",
      "id" -> videoId,
      "key" -> apiKey
    )

    val request = basicRequest
      .get(uri"$baseUrl?$params")
      .response(asString)

    val backend = HttpClientSyncBackend()

    val response = try {
      request.send(backend)
    } catch {
      case ex: Exception =>
        println(s"Error fetching video details for $videoId: ${ex.getMessage}")
        backend.close()
        return None
    }

    backend.close()

    if (response.code.isSuccess) {
      val json = Json.parse(response.body.getOrElse(""))
      val items = (json \ "items").asOpt[JsArray].getOrElse(JsArray())

      if (items.value.isEmpty) {
        println(s"No details found for video ID: $videoId")
        None
      } else {
        val item = items(0)
        val snippet = (item \ "snippet")
        val statistics = (item \ "statistics")

        Some(VideoDetails2(
          videoId = (item \ "id").as[String],
          title = (snippet \ "title").asOpt[String].getOrElse("No Title"),
          viewCount = (statistics \ "viewCount").asOpt[String].getOrElse("0"),
          likeCount = (statistics \ "likeCount").asOpt[String].getOrElse("0"),
          commentCount = (statistics \ "commentCount").asOpt[String].getOrElse("0")
        ))
      }
    } else {
      println(s"Failed to fetch video details for $videoId: ${response.statusText}")
      None
    }
  }

  // Write video details to CSV
  def writeToCSV(video: VideoDetails2, category: String, writer: BufferedWriter): Unit = {
    writer.write(
      s"""\n"${video.videoId}","${video.title.replace("\"", "\"\"")}","${video.viewCount}","${video.likeCount}","${video.commentCount}","$category""""
    )
    writer.flush()  // Ensure that data is written to the file immediately
  }

  // Function to read video IDs from a file
  def readVideoIdsFromFile(filePath: String): Seq[String] = {
    val source = Source.fromFile(filePath)
    try {
      source.getLines().toSeq.filter(_.nonEmpty) // Read lines and ignore empty ones
    } finally {
      source.close()
    }
  }

  // Function to process and save video details for each category
  def processCategoryAndSave(category: String, path: String, writer: BufferedWriter): Unit = {
    try {
      println(s"Processing category: $category, File: $path")
      val videoIds = readVideoIdsFromFile(path)
      videoIds.foreach { id =>
        try {
          fetchVideoDetails2(id) match {
            case Some(video) =>
              writeToCSV(video, category, writer)
              println(s"Successfully written details for video ID: $id in category: $category")
            case None =>
              println(s"No details found for video ID: $id in category: $category")
          }
        } catch {
          case e: Exception =>
            println(s"Error processing video ID $id: ${e.getMessage}")
        }
      }
    } catch {
      case e: Exception =>
        println(s"Error reading file $path for category $category: ${e.getMessage}")
    }
  }

  // Main function to run the task every 15 seconds
  def main(args: Array[String]): Unit = {
    val filePaths = Map(
      "Music" -> "D:/GWU_World/Practice/Music_IDs.txt",
      "Gaming" -> "D:/GWU_World/Practice/Gaming_IDs.txt",
      "Technology" -> "D:/GWU_World/Practice/Technology_IDs.txt",
      "Entertainment" -> "D:/GWU_World/Practice/Entertainment_IDs.txt",
      "Sports" -> "D:/GWU_World/Practice/Sports_IDs.txt",
      "News" -> "D:/GWU_World/Practice/News_IDs.txt"
    )

    val fileName = "YouTubeData3.csv"
    val file = new File(fileName)

    // Run the task periodically every 15 seconds using do-while loop
    do {
      println("Starting a new cycle of fetching and saving video details...")

      // Delete the file if it exists (start fresh)
      if (file.exists()) {
        if (file.delete()) {
          println(s"Deleted existing file: $fileName")
        } else {
          println(s"Failed to delete existing file: $fileName")
        }
      }

      // Open the file in overwrite mode (use 'false' to ensure we don't append)
      val writer = new BufferedWriter(new FileWriter(file, false)) // Overwrite mode

      try {
        // Write the header only if the file is empty
        if (file.length() == 0) {
          writer.write("Video ID,Title,View Count,Like Count,Comment Count,Category") // Description, Published At, Channel Title
        }

        // Process each category and its corresponding video IDs file
        filePaths.foreach { case (category, path) =>
          processCategoryAndSave(category, path, writer)
        }

      } catch {
        case e: Exception =>
          println(s"Error during processing: ${e.getMessage}")
      } finally {
        // Ensure writer is flushed and closed after each cycle and eventually closed
        writer.flush()
        writer.close()
        println(s"All video details saved to $fileName")
      }

      // Initialize Spark Session in local mode
      val spark = SparkSession.builder()
        .master("local[*]")
        .appName("YouTubeAnalysis")
        .getOrCreate()

      // Path to the input CSV file
      val inputPath = "D:/GWU_World/Practice/YouTubeData.csv"

      val outputPath = "D:/GWU_World/Practice/YouTubeAnalysis.csv"
      val writer1 = new PrintWriter(new File(outputPath))
      println(s"Writer1 initialized for file: $outputPath")

      // Load the CSV into a DataFrame
      val videoData = spark.read.option("header", "true").option("inferSchema", "true").csv(inputPath)

      // Descriptive Statistics
      // 1. Calculate highest, lowest, and average view counts
      val popularityStats = videoData.agg(
        max("View Count").as("Max_View_Count"),
        min("View Count").as("Min_View_Count"),
        avg("View Count").as("Avg_View_Count")
      )

      // 2. Calculate Engagement Metrics
      val engagementMetrics = videoData
        .withColumn("Like/ViewRatio", col("Like Count") / col("View Count"))
        .withColumn("Comment/LikeRatio", col("Comment Count") / col("Like Count"))
        .withColumn("Comment/ViewRatio", col("Comment Count") / col("View Count"))

      // Category-Based Analysis
      // 3. Average metrics per category
      val categoryMetrics = videoData.groupBy("Category").agg(
        avg("View Count").as("Avg_View_Count"),
        avg("Like Count").as("Avg_Like_Count"),
        avg("Comment Count").as("Avg_Comment_Count")
//        count("*").as("Total Videos")
      )
      val collectedData = categoryMetrics.collect()

      // 4. Most popular category by views, likes, and comments
      val popularCategory = categoryMetrics
        .orderBy(desc("Avg_View_Count"), desc("Avg_Like_Count"), desc("Avg_Comment_Count"))
        .limit(1)

      val validCategories = Set("Gaming", "Entertainment", "Sports", "Music", "News", "Technology")
      try {
        // Check if there is data to write
        if (collectedData.isEmpty) {
          println("No data to write to the file.")
        } else {
          // Write the header
          val header = categoryMetrics.columns.mkString(",")
          println(s"Writing header: $header")
          writer1.println(header)

          // Write each row
          collectedData.zipWithIndex.foreach { case (row, index) =>
            val category = row.getAs[String]("Category")
            if (validCategories.contains(category)) {
              val rowStr = row.toSeq.map(_.toString).mkString(",")
              println(s"Writing row $index: $rowStr")
              writer1.println(rowStr)
              writer1.flush()
            } else {
              println(s"Skipping row $index for category '$category'")
            }
          }
        }
      } catch {
        case ex: Exception =>
          println(s"Error during writing: ${ex.getMessage}")
      } finally {
        writer.close()
        println(s"Data written successfully to $outputPath")
      }
      // Stop the Spark session
      spark.stop()

      val spark1 = SparkSession.builder()
        .appName("CSV to MySQL")
        .master("local[*]")
        .getOrCreate()

      val csvFilePath = "D:/GWU_World/Practice/YouTubeAnalysis.csv"

      val df = spark1.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(csvFilePath)

      println("Schema of the DataFrame:")
      df.printSchema()

      println("Data in the DataFrame:")
      df.show(5)

      val jdbcUrl = "jdbc:mysql://localhost:3306/YoutubeStatsDb"
      val connectionProperties = new java.util.Properties()
      connectionProperties.setProperty("user", "root")
      connectionProperties.setProperty("password", "YOUR_MYSQL_PASSWORD")
      connectionProperties.setProperty("driver", "com.mysql.cj.jdbc.Driver")

      try {
        df.write
          .mode("overwrite")
          .jdbc(jdbcUrl, "YoutubeStatsDb", connectionProperties)
        println("Data written successfully to the database!")
      } catch {
        case e: Exception =>
          println("Error occurred while writing to MySQL:")
          e.printStackTrace()
      }

      println("Sleeping for 2 Min...")
      Thread.sleep(120000) // 2 Min
    } while (true) // Infinite loop to keep repeating the task
  }
}
