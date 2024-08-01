package org.apache.pinot.connector.spark.v3.datasource

import org.apache.pinot.spi.data.readers.{GenericRow, RecordReader, RecordReaderConfig}

import java.io.File
import java.util

/**
 * A buffered record reader that stores the records in memory and allows for iteration over them.
 * This is useful to satisfy the RecordReader interface in Pinot, and also allow for Spark executor
 * to write records.
 *
 * To improve resilience, implementation can be improved to write records to disk when memory is full,
 * but for now, this is a simple in-memory implementation.
 */
class PinotBufferedRecordReader extends RecordReader {
  private val recordBuffer = new util.ArrayList[GenericRow]()
  private var readCursor = 0

  def init(dataFile: File, fieldsToRead: util.Set[String], recordReaderConfig: RecordReaderConfig): Unit = {
    // Do nothing.
  }

  def write(record: GenericRow): Unit = {
    recordBuffer.add(record)
  }

  def hasNext: Boolean = {
    readCursor < recordBuffer.size()
  }

  def next(): GenericRow = {
    readCursor += 1
    recordBuffer.get(readCursor - 1)
  }

  def next(reuse: GenericRow): GenericRow = {
    readCursor += 1
    reuse.clear()
    reuse.init(recordBuffer.get(readCursor - 1).copy())
    reuse
  }

  def rewind(): Unit = {
    readCursor = 0
  }

  def close(): Unit = {
    recordBuffer.clear()
  }
}
