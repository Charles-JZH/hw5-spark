import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Main {
  def main(args: Array[String]): Unit = {
    val inputFiles = "src/main/resources/Data/Hugo/*," +
      "src/main/resources/Data/shakespeare/*," +
      "src/main/resources/Data/Tolstoy/*"

    val stopWordFile = "src/main/resources/Data/stopWords.txt"

    val outputPath = "src/main/resources/Data/output"

    val appName = "WordCount"
    val conf = new SparkConf().setAppName(appName).setMaster("local")
    val sc = new SparkContext(conf)

    val stopWords = sc.textFile(stopWordFile).flatMap(x => x.split("\\r?\\n")).map(_.trim)
    val broadcastStopWords = sc.broadcast(stopWords.collect.toSet)

    val inputRDD = sc.wholeTextFiles(inputFiles)
    val inputRDDv1 = inputRDD.flatMap({
      case (path, content) =>
        content.replaceAll("[^\\w\\s]|('s|ly|ed|ing|ness) ", " ")
          .split("""\W+""")
          .filter(!broadcastStopWords.value.contains(_)) map {
          word => (word, path)
        }
    })
    val inputRDDv2 = inputRDDv1.map(x => (x, 1))
    val inputRDDv3 = inputRDDv2.reduceByKey(_ + _)
    val inputRDDv4 = inputRDDv3.map({
      case ((word, path), c) => (word, (path, c))
    })
    val inputRDDv5 = inputRDDv4.groupBy({
      case (word, (_, _)) => word
    })
    val inputRDDv6 = inputRDDv5.map({
      case (word, res) =>
        val result = res map {
          case (_, (p, n)) => (p, n)
        }
        (word, result.mkString(", "))
    })
    inputRDDv6.saveAsTextFile(outputPath)
  }
}