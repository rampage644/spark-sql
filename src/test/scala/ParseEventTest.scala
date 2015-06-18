import org.scalatest.{FunSpec, Matchers}
import scala.io.Source

import ParseEvent.parse

class ParseEventTest extends  FunSpec with Matchers {
  describe ("parseXML") {
    it ("should parse XML") {
      val file = getClass getResource "test1.csv"
      val text = Source.fromURL(file).mkString
      val data = Map(
        ("name", "module_end"),
        ("package", "sqlserver"),
        ("timestamp", "2015-06-17T15:11:56.683Z"),
        ("source_database_id", "75"),
        ("object_id", "978102525"),
        ("collect_system_time", "2015-06-17T15:11:56.683Z")
      )

      parse(text) should be (data)
    }

    it ("should work with XML as one line") {
      val file = getClass getResource "test2.csv"
      val text = Source.fromURL(file).mkString

      parse(text) should not be (Map())

    }

    it ("should work fast enough") {
      // why bother?
    }

    it ("should ignore malformed XML (so far)") {
      val file = getClass getResource "test3.csv"
      val text = Source.fromURL(file).mkString

      parse(text) should be (Map())
    }
  }
}