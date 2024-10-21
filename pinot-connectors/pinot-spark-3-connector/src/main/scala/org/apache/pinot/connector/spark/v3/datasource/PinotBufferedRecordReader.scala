/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.connector.spark.v3.datasource

import org.apache.pinot.spi.data.readers.{GenericRow, RecordReader, RecordReaderConfig}

import java.io.File
import java.util

/**
 * A buffered record reader that stores the records in memory and allows for iteration over them.
 * This is used to satisfy the RecordReader interface in Pinot, as well as allowing Spark executor
 * to write records.
 *
 * TODO: To improve resilience, write records to disk when memory is full.
 */
class PinotBufferedRecordReader extends RecordReader {
  private val recordBuffer = new util.ArrayList[GenericRow]()
  private var readCursor = 0

  def init(dataFile: File, fieldsToRead: util.Set[String], recordReaderConfig: RecordReaderConfig): Unit = {
    // Do nothing.
    // TODO: Honor 'fieldsToRead' parameter to avoid ingesting unwanted fields.
  }

  def write(record: GenericRow): Unit = {
    recordBuffer.add(record)
  }

  def hasNext: Boolean = {
    readCursor < recordBuffer.size()
  }

  override def next(): GenericRow = {
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
