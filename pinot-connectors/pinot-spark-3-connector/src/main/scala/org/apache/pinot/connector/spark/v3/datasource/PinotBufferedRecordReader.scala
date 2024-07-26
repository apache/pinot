package org.apache.pinot.connector.spark.v3.datasource

import org.apache.pinot.spi.data.readers.{GenericRow, RecordReader, RecordReaderConfig}

import java.io.File
import java.util

class PinotBufferedRecordReader extends RecordReader {
  private val recordBuffer = new util.ArrayList[GenericRow]()
  private var cursor = 0

  def init(dataFile: File, fieldsToRead: util.Set[String], recordReaderConfig: RecordReaderConfig): Unit = {
    // do nothing
  }

  def write(record: GenericRow): Unit = {
    recordBuffer.add(record)
  }

  def hasNext: Boolean = {
    cursor < recordBuffer.size()
  }

  def next(): GenericRow = {
    cursor += 1
    recordBuffer.get(cursor - 1)
  }

  def next(reuse: GenericRow): GenericRow = {
    cursor += 1
    reuse.clear()
    reuse.init(recordBuffer.get(cursor - 1).copy())
    reuse
  }

  def rewind(): Unit = {
    cursor = 0
  }

  def close(): Unit = {
    recordBuffer.clear()
  }
}
