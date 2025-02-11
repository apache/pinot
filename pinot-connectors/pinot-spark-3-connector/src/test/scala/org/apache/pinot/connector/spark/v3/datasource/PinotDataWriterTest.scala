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

import org.apache.pinot.connector.spark.common.PinotDataSourceWriteOptions
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType, BinaryType}
import org.apache.spark.sql.connector.write.WriterCommitMessage
import org.scalatest.matchers.should.Matchers
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.pinot.common.utils.TarCompressionUtils
import org.apache.pinot.spi.data.readers.GenericRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.net.URI
import java.nio.file.{Files, Paths}
import scala.io.Source

class PinotDataWriterTest extends AnyFunSuite with Matchers with BeforeAndAfter {

  var tmpDir: File = _

  before {
    tmpDir = Files.createTempDirectory("pinot-spark-connector-write-test").toFile
  }

  after {
    if (tmpDir.exists()) {
      tmpDir.listFiles().foreach(_.delete())
      tmpDir.delete()
    }
  }

  test("Initialize buffer and accept records") {
    val writeOptions = PinotDataSourceWriteOptions(
      tableName = "testTable",
      savePath = "/tmp/pinot",
      timeColumnName = "ts",
      timeFormat = "EPOCH|SECONDS",
      timeGranularity = "1:SECONDS",
      segmentNameFormat = "{table}_{partitionId:03}",
      invertedIndexColumns = Array("name"),
      noDictionaryColumns = Array("age"),
      bloomFilterColumns = Array("name"),
      rangeIndexColumns = Array()
    )
    val writeSchema = StructType(Seq(
      StructField("name", StringType, nullable = false),
      StructField("age", IntegerType, nullable = false),
      StructField("ts", LongType, nullable = false),
      StructField("bin", BinaryType, nullable = false),
    ))

    val pinotSchema = SparkToPinotTypeTranslator.translate(
      writeSchema, writeOptions.tableName, writeOptions.timeColumnName,
      writeOptions.timeFormat, writeOptions.timeGranularity)
    val writer = new PinotDataWriter[InternalRow](0, 0, writeOptions, writeSchema, pinotSchema)

    val record1 = new TestInternalRow(Array[Any]("Alice", 30, 1234567890L, "Alice".getBytes))
    val record2 = new TestInternalRow(Array[Any]("Bob", 25, 1234567891L, "Bob".getBytes))

    writer.write(record1)
    writer.write(record2)

    val writeBuffer = writer.bufferedRecordReader
    writer.bufferedRecordReader.hasNext shouldBe true
    writeBuffer.next() shouldBe a[GenericRow]
    writeBuffer.next() shouldBe a[GenericRow]

    writer.close()
    writeBuffer.hasNext shouldBe false
  }

  test("Should create segment file on commit") {
    // create tmp directory with test name
    tmpDir = Files.createTempDirectory("pinot-spark-connector-test").toFile

    val writeOptions = PinotDataSourceWriteOptions(
      tableName = "testTable",
      savePath = tmpDir.getAbsolutePath,
      timeColumnName = "ts",
      timeFormat = "EPOCH|SECONDS",
      timeGranularity = "1:SECONDS",
      segmentNameFormat = "{table}_{startTime}_{endTime}_{partitionId:03}",
      invertedIndexColumns = Array("name"),
      noDictionaryColumns = Array("age"),
      bloomFilterColumns = Array("name"),
      rangeIndexColumns = Array()
    )
    val writeSchema = StructType(Seq(
      StructField("name", StringType, nullable = false),
      StructField("age", IntegerType, nullable = false),
      StructField("ts", LongType, nullable = false),
      StructField("bin", BinaryType, nullable = false),
    ))
    val pinotSchema = SparkToPinotTypeTranslator.translate(
      writeSchema, writeOptions.tableName, writeOptions.timeColumnName,
      writeOptions.timeFormat, writeOptions.timeGranularity)
    val writer = new PinotDataWriter[InternalRow](0, 0, writeOptions, writeSchema, pinotSchema)
    val record1 = new TestInternalRow(Array[Any]("Alice", 30, 1234567890L, "Alice".getBytes))
    writer.write(record1)
    val record2 = new TestInternalRow(Array[Any]("Bob", 25, 1234567891L, "Bob".getBytes))
    writer.write(record2)

    val commitMessage: WriterCommitMessage = writer.commit()
    commitMessage shouldBe a[SuccessWriterCommitMessage]

    // Verify that the segment is created and stored in the target location
    val fs = FileSystem.get(new URI(writeOptions.savePath), new org.apache.hadoop.conf.Configuration())
    val segmentPath = new Path(writeOptions.savePath + "/testTable_1234567890_1234567891_000.tar.gz")
    fs.exists(segmentPath) shouldBe true

    // Verify the contents of the segment tar file
    TarCompressionUtils.untar(
      new File(writeOptions.savePath + "/testTable_1234567890_1234567891_000.tar.gz"),
      new File(writeOptions.savePath))
    val untarDir = Paths.get(writeOptions.savePath + "/testTable_1234567890_1234567891_000/v3/")
    Files.exists(untarDir) shouldBe true

    val segmentFiles = Files.list(untarDir).toArray.map(_.toString)
    segmentFiles should contain (untarDir + "/creation.meta")
    segmentFiles should contain (untarDir + "/index_map")
    segmentFiles should contain (untarDir + "/metadata.properties")
    segmentFiles should contain (untarDir + "/columns.psf")

    // Verify basic metadata content
    val metadataSrc = Source.fromFile(untarDir + "/metadata.properties")
    val metadataContent = metadataSrc.getLines.mkString("\n")
    metadataSrc.close()

    metadataContent should include ("segment.name = testTable_1234567890_1234567891_000")
    metadataContent should include ("segment.time.column.name = ts")
    metadataContent should include ("segment.start.time = 1234567890")
    metadataContent should include ("segment.end.time = 1234567891")
  }

  test("getSegmentName should format segment name correctly with custom format") {
    val testCases = Seq(
      ("{table}_{partitionId}", "airlineStats_12"),
      ("{partitionId:05}_{table}", "00012_airlineStats"),
      ("{table}_20240805", "airlineStats_20240805"),
      ("{table}_{startTime}_{endTime}_{partitionId:03}", "airlineStats_1234567890_1234567891_012"),
    )

    testCases.foreach { case (format, expected) =>
      val writeOptions = PinotDataSourceWriteOptions(
        tableName = "airlineStats",
        savePath = "/tmp/pinot",
        timeColumnName = "ts",
        timeFormat = "EPOCH|SECONDS",
        timeGranularity = "1:SECONDS",
        segmentNameFormat = format,
        invertedIndexColumns = Array("name"),
        noDictionaryColumns = Array("age"),
        bloomFilterColumns = Array("name"),
        rangeIndexColumns = Array(),
      )
      val writeSchema = StructType(Seq(
        StructField("name", StringType, nullable = false),
        StructField("age", IntegerType, nullable = false),
        StructField("ts", LongType, nullable = false),
      ))

      val pinotSchema = SparkToPinotTypeTranslator.translate(
        writeSchema, writeOptions.tableName, writeOptions.timeColumnName,
        writeOptions.timeFormat, writeOptions.timeGranularity)
      val writer = new PinotDataWriter[InternalRow](12, 0, writeOptions, writeSchema, pinotSchema)
      writer.write(new TestInternalRow(Array[Any]("Alice", 30, 1234567890L)))
      writer.write(new TestInternalRow(Array[Any]("Bob", 25, 1234567891L)))

      val segmentName = writer.getSegmentName

      segmentName shouldBe expected
    }
  }
}

private class TestInternalRow(values: Array[Any]) extends GenericInternalRow(values) {
  override def getString(ordinal: Int): String = values(ordinal).asInstanceOf[String]
}
