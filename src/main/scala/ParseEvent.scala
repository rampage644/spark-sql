import scala.util.Try
import scala.xml.XML

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.{StructType,StructField,StringType};

class Parser extends Serializable {
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
  case class SRow(
    physical_reads: Int = 0,
    duration: Int = 0,
    query_hash: String = "",
    name: String = "",
    timestamp: String = "",
    collect_system_time: String = "",
    logical_reads: Int = 0,
    object_name: String = "",
    last_row_count: Int = 0,
    object_type: String = "",
    statement: String = "",
    line_number: Int = 0,
    row_count: Int = 0,
    source_database_id: Int = 0,
    offset: Int = 0,
    nt_username: String = "",
    offset_end: Int = 0,
    database_name: String = "",
    cpu_time: Int = 0,
    client_hostname: String = "",
    writes: Int = 0,
    query_plan_hash: String = "",
    object_id: String = "",
    parameterized_plan_handle: String = "",
    `package`: String = "") {
  }

  def createSRow(values: Map[String, String]) = {
    SRow(
      values.getOrElse("physical_reads", "0").toInt,
      values.getOrElse("duration", "0").toInt,
      values.getOrElse("query_hash", ""),
      values.getOrElse("name", ""),
      values.getOrElse("timestamp", ""),
      values.getOrElse("collect_system_time", ""),
      values.getOrElse("logical_reads", "0").toInt,
      values.getOrElse("object_name", ""),
      values.getOrElse("last_row_count", "0").toInt,
      values.getOrElse("object_type", ""),
      values.getOrElse("statement", ""),
      values.getOrElse("line_number", "0").toInt,
      values.getOrElse("row_count", "0").toInt,
      values.getOrElse("source_database_id", "0").toInt,
      values.getOrElse("offset", "0").toInt,
      values.getOrElse("nt_username", ""),
      values.getOrElse("offset_end", "0").toInt,
      values.getOrElse("database_name", ""),
      values.getOrElse("cpu_time", "0").toInt,
      values.getOrElse("client_hostname", ""),
      values.getOrElse("writes", "0").toInt,
      values.getOrElse("query_plan_hash", ""),
      values.getOrElse("object_id", ""),
      values.getOrElse("parameterized_plan_handle", ""),
      values.getOrElse("package", "")
    )
  }

  def main(args: Array[String]) {
    val input = "/home/ramp/tmp/test.csv"
    val conf = new SparkConf().setAppName("SQL-on-xml")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val parser = new Parser()
    val table = sc.textFile(input, 2).flatMap(parser.parseIncrementally).map(createSRow).toDF
    table.saveAsParquetFile("srows.parquet")
  }
}
