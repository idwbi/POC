import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
 
import scala.xml.XML
 
object PoCRanking {
 
  def main(args: Array[String]) {
    val inputPath = "xxx.xml"
 
    val conf = new SparkConf().setAppName("PoCRanking")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
 
 
    val pathRDD = sc.wholeTextFiles(inputPath)
    val files = pathRDD.map { case (filename, content) => (filename.substring(56,62), content)}
 
    val xml10pattern = "[^"+ "\u0009\r\n" + "\u0020-\uD7FF" + "\uE000-\uFFFD" + "\ud800\udc00-\udbff\udfff" + "]"
    val xml11pattern = "[^" + "\u0001-\uD7FF" + "\uE000-\uFFFD" + "\ud800\udc00-\udbff\udfff" + "]+"
 
 
    val texts = files.map{s =>
      val genre = s._1.toString
      val xml = XML.loadString(s._2.toString.replaceAll(xml11pattern, ""))
      val items = xml \\ "item"
      val rankData = items.map(
        xmlNode => (
          (xmlNode \ "shopid").text.toString.reverse.padTo(11,"0").reverse.mkString.concat("-").concat((xmlNode \ "itemid").text.toString.reverse.padTo(11,"0").reverse.mkString.concat("-1")), //$key
          genre,
          (xmlNode \ "rank").text.toInt,
          (xmlNode \ "genreid").text.toString.split('/').indexOf(genre) - 1  //depth
        )) filter(_._3 <= 20)
      rankData.toList
    } flatMap(x => x) map{ case (a, b, c, d) => (a, (b, c, d))}
 
    val rankingRDD = texts.aggregateByKey(List[Any]())(
      (aggr, value) => aggr ::: (value :: Nil),
      (aggr1, aggr2) => aggr1 ::: aggr2
    )
 
    def toJson(data:Tuple2[String, List[Any]]):String = {
      val key = "$key"
      val genre = data._2.map{ v =>
        v match {
          case (a,b,c) => s"""{"genre":$a, "rank":$b, "depth":$c, "start_date":"2017-07-21T10:00:00.000+09:00", "end_date":"2017-07-22T9:59:59.999+09:00","period":1}"""
        }
      }.mkString(",")
 
      var res = s"""
       {
         "$key":"${data._1}",
         "ranking":[$genre]
       },
      """
      res
    }
 
    rankingRDD.map(toJson).saveAsTextFile("poc-ranking/daily_json")
 
  }
}