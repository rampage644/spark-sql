import scala.util.Try
import scala.xml.XML

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import org.apache.spark.sql.SQLContext

class Parser {
  private var buffer: List[String] = Nil

  private def parse(text:String) : Option[Map[String, String]] = {
    val tryRoot = Try(XML.loadString(text))

    if (tryRoot.isFailure)
      return None

    val root = tryRoot.get
    val attributes = root.attributes.map(el => (el.key, el.value.text))

    val data = root \ "data" map (_ \ "@name") map (_.text) zip (root \ "data" \ "value" map (_.text))
    val action = root \ "action" map (_ \ "@name") map (_.text) zip (root \ "action" \ "value" map (_.text))

    Some(attributes.toMap ++ data ++ action)
  }


  private def removeBOM(text: String) = text.dropWhile(_ != '<')

  def parseIncrementally(text: String) : Option[Map[String,String]] = {
    (hasStartingTag(text), hasClosingTag(text)) match {
      case (true, true) => parse(removeBOM(text))
      case (false, true) => parse(buffer :+ text mkString "\n")
      case (true, false) => {
        buffer = removeBOM(text) :: Nil
        None
      }
      case (false, false) => {
        buffer = buffer :+ text
        None
      }
    }
  }

  private def hasStartingTag(text: String) = text.contains("<event")
  private def hasClosingTag(text: String) = text.contains("</event>")
}

object ParseEvent {
  case class SRow(nt_username: String, row_count: Int)

  def main(args: Array[String]) {
    val input = "/home/ramp/tmp/test.csv"
    val conf = new SparkConf().setAppName("SQL-on-xml")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._
    val parser = new Parser()
    val table = sc.textFile(input, 2).flatMap(parser.parseIncrementally).map(values => SRow(values("nt_username"), values("row_count").toInt)).toDF
    table.registerTempTable("SRows")
    val res = sqlContext.sql("SELECT count(*) FROM SRows")
    print (res.collectAsList())
  }
}
