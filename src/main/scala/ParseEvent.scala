import scala.util.Try
import scala.xml.XML

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import org.apache.spark.sql.SQLContext

object ParseEvent {
  def parse(text:String) : Map[String, String] = {
    val tryRoot = Try(XML.loadString(text.dropWhile(_ != '<')))

    if (tryRoot.isFailure)
      return Map()

    val root = tryRoot.get
    val attributes = root.attributes.map(el => (el.key, el.value.text))

    val data = root \ "data" map (_ \ "@name") map (_.text) zip (root \ "data" \ "value" map (_.text))
    val action = root \ "action" map (_ \ "@name") map (_.text) zip (root \ "action" \ "value" map (_.text))

    attributes.toMap ++ data ++ action
  }

  case class SRow(nt_username: String, row_count: Int)

  def main(args: Array[String]) {
    val input = "/home/ramp/tmp/test100.csv"
    val conf = new SparkConf().setAppName("SQL-on-xml")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._
    val table = sc.textFile(input, 2).map(parse).filter(!_.isEmpty).map(values => SRow(values("nt_username"), values("row_count").toInt)).toDF
    table.registerTempTable("SRows")
    val res = sqlContext.sql("SELECT count(*) FROM SRows")
    print (res.collectAsList())
  }
}
