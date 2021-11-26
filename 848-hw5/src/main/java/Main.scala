import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Main {
  def main(args: Array[String]): Unit = {
    val inputFiles: List[String] = List("src/main/resources/Data/Hugo/Miserables.txt",
    "src/main/resources/Data/Hugo/NotreDame_De_Paris.txt",
    "src/main/resources/Data/shakespeare/comedies/allswellthatendswell",
    "src/main/resources/Data/shakespeare/comedies/asyoulikeit",
    "src/main/resources/Data/shakespeare/comedies/comedyoferrors",
    "src/main/resources/Data/shakespeare/comedies/cymbeline",
    "src/main/resources/Data/shakespeare/comedies/loveslabourslost",
    "src/main/resources/Data/shakespeare/comedies/measureforemeasure",
    "src/main/resources/Data/shakespeare/comedies/merchantofvenice",
    "src/main/resources/Data/shakespeare/comedies/merrywivesofwindsor",
    "src/main/resources/Data/shakespeare/comedies/midsummersnightsdream",
    "src/main/resources/Data/shakespeare/comedies/muchadoaboutnothing",
    "src/main/resources/Data/shakespeare/comedies/periclesprinceoftyre",
    "src/main/resources/Data/shakespeare/comedies/tamingoftheshrew",
    "src/main/resources/Data/shakespeare/comedies/tempest",
    "src/main/resources/Data/shakespeare/comedies/troilusandcressida",
    "src/main/resources/Data/shakespeare/comedies/twelfthnight",
    "src/main/resources/Data/shakespeare/comedies/twogentlemenofverona",
    "src/main/resources/Data/shakespeare/comedies/winterstale",
    "src/main/resources/Data/shakespeare/glossary",
    "src/main/resources/Data/shakespeare/histories/1kinghenryiv",
    "src/main/resources/Data/shakespeare/histories/1kinghenryvi",
    "src/main/resources/Data/shakespeare/histories/2kinghenryiv",
    "src/main/resources/Data/shakespeare/histories/2kinghenryvi",
    "src/main/resources/Data/shakespeare/histories/3kinghenryvi",
    "src/main/resources/Data/shakespeare/histories/kinghenryv",
    "src/main/resources/Data/shakespeare/histories/kinghenryviii",
    "src/main/resources/Data/shakespeare/histories/kingjohn",
    "src/main/resources/Data/shakespeare/histories/kingrichardii",
    "src/main/resources/Data/shakespeare/histories/kingrichardiii",
    "src/main/resources/Data/shakespeare/poetry/loverscomplaint",
    "src/main/resources/Data/shakespeare/poetry/rapeoflucrece",
    "src/main/resources/Data/shakespeare/poetry/sonnets",
    "src/main/resources/Data/shakespeare/poetry/various",
    "src/main/resources/Data/shakespeare/poetry/venusandadonis",
    "src/main/resources/Data/shakespeare/README",
    "src/main/resources/Data/shakespeare/tragedies/antonyandcleopatra",
    "src/main/resources/Data/shakespeare/tragedies/coriolanus",
    "src/main/resources/Data/shakespeare/tragedies/hamlet",
    "src/main/resources/Data/shakespeare/tragedies/juliuscaesar",
    "src/main/resources/Data/shakespeare/tragedies/kinglear",
    "src/main/resources/Data/shakespeare/tragedies/macbeth",
    "src/main/resources/Data/shakespeare/tragedies/othello",
    "src/main/resources/Data/shakespeare/tragedies/romeoandjuliet",
    "src/main/resources/Data/shakespeare/tragedies/timonofathens",
    "src/main/resources/Data/shakespeare/tragedies/titusandronicus",
    "src/main/resources/Data/Tolstoy/anna_karenhina.txt",
    "src/main/resources/Data/Tolstoy/war_and_peace.txt")

    val stopWordFile = "src/main/resources/Data/stopWords.txt"

    val theyMap = new scala.collection.mutable.HashMap[String, String]
    val sheMap = new scala.collection.mutable.HashMap[String, String]
    val heMap = new scala.collection.mutable.HashMap[String, String]
    val itMap = new scala.collection.mutable.HashMap[String, String]
    val theMap = new scala.collection.mutable.HashMap[String, String]
    val asMap = new scala.collection.mutable.HashMap[String, String]
    val isMap = new scala.collection.mutable.HashMap[String, String]
    val andMap = new scala.collection.mutable.HashMap[String, String]
    val finalMap = new scala.collection.mutable.HashMap[String, scala.collection.mutable.HashMap[String, String]]
    finalMap += ("they" -> theyMap)
    finalMap += ("she" -> sheMap)
    finalMap += ("he" -> heMap)
    finalMap += ("it" -> itMap)
    finalMap += ("the" -> theMap)
    finalMap += ("as" -> asMap)
    finalMap += ("is" -> isMap)
    finalMap += ("and" -> andMap)

    val appName = "WordCount"
    val conf = new SparkConf().setAppName(appName).setMaster("local")
    val sc = new SparkContext(conf)

    val stopRDD = sc.textFile(stopWordFile)
    val stopList = stopRDD.map(x => x.trim()).collect()
    val targetList: Array[String] = Array[String]("""\t\().,?[]!;|""")

    def replaceAndSplit(s: String): Array[String] = {
      for(c <- targetList)
        s.replace(c, " ")
      s.split("\\s+")
    }

    for (inputFile <- inputFiles) {
      val inputRDD = sc.textFile(inputFile)
      val inputRDDv1 = inputRDD.flatMap(replaceAndSplit)
      val inputRDDv2 = inputRDDv1.filter(x => stopList.contains(x))
      val inputRDDv3 = inputRDDv2.map(x => (x,1))
      val inputRDDv4 = inputRDDv3.reduceByKey(_ + _)
      val resMap = inputRDDv4.collect().toMap
      val keySet = resMap.keys
      val keyIterator = keySet.iterator
      while (keyIterator.hasNext) {
        val key = keyIterator.next
        finalMap(key) += (inputFile -> resMap(key).toString)
      }
    }

    val finalKeySet = finalMap.keys
    val finalKeyIterator = finalKeySet.iterator
    while (finalKeyIterator.hasNext) {
      val key = finalKeyIterator.next
      println(key)
      val subMap = finalMap(key)
      val subKeySet = subMap.keys
      val subKeyIterator = subKeySet.iterator
      while (subKeyIterator.hasNext) {
        val subKey = subKeyIterator.next
        println(subKey + ":" + subMap(subKey))
      }
      println("")
    }
  }
}