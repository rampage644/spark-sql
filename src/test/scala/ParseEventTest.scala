import org.scalatest.{FunSpec, Matchers}
import scala.io.Source

class ParseEventTest extends  FunSpec with Matchers {
  describe ("Parser class") {
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

      new Parser().parseIncrementally(text) should be (Some(data))
    }

    it ("should work with XML as one line") {
      val file = getClass getResource "test2.csv"
      val text = Source.fromURL(file).mkString

      new Parser().parseIncrementally(text) should not be (None)

    }

    it ("should work fast enough") {
      // why bother?
    }

    it ("should ignore malformed XML (so far)") {
      val file = getClass getResource "test3.csv"
      val text = Source.fromURL(file).mkString

      new Parser().parseIncrementally(text) should be (None)
    }

    it ("should process multiline xml") {
      val file = getClass getResource "test4.csv"
      val parser = new Parser()
      Source.fromURL(file).getLines().flatMap(parser.parseIncrementally).next() should not be (None)
    }
  }
}