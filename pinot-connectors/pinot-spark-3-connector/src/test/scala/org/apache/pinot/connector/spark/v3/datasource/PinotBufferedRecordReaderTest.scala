package org.apache.pinot.connector.spark.v3.datasource

import org.apache.pinot.spi.data.readers.GenericRow
import org.scalatest.funsuite.AnyFunSuite

class PinotBufferedRecordReaderTest extends AnyFunSuite {

  test("Write and read records back") {
    val reader = new PinotBufferedRecordReader()
    val record1 = new GenericRow()
    record1.putValue("field1", "value1")
    val record2 = new GenericRow()
    record2.putValue("field2", "value2")

    reader.write(record1)
    reader.write(record2)

    assert(reader.hasNext)
    assert(reader.next() == record1)
    assert(reader.hasNext)
    assert(reader.next() == record2)
    assert(!reader.hasNext)
  }

  test("Rewind and read records again") {
    val reader = new PinotBufferedRecordReader()
    val record1 = new GenericRow()
    record1.putValue("field1", "value1")
    val record2 = new GenericRow()
    record2.putValue("field2", "value2")

    reader.write(record1)
    reader.write(record2)

    reader.rewind()

    assert(reader.hasNext)
    assert(reader.next() == record1)
    assert(reader.hasNext)
    assert(reader.next() == record2)
    assert(!reader.hasNext)
  }

  test("Close and clear buffer") {
    val reader = new PinotBufferedRecordReader()
    val record1 = new GenericRow()
    record1.putValue("field1", "value1")

    reader.write(record1)
    reader.close()

    assert(!reader.hasNext)
  }

  test("Next with reuse") {
    val reader = new PinotBufferedRecordReader()
    val record1 = new GenericRow()
    record1.putValue("field1", "value1")
    val record2 = new GenericRow()
    record2.putValue("field2", "value2")

    reader.write(record1)
    reader.write(record2)

    val reuse = new GenericRow()
    reader.rewind()

    assert(reader.hasNext)
    assert(reader.next(reuse) == record1)
    assert(reader.hasNext)
    assert(reader.next(reuse) == record2)
    assert(!reader.hasNext)
  }
}