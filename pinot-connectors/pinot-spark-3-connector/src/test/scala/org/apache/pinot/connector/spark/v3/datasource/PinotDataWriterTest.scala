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
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, ByteType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructField, StructType}
import org.apache.spark.sql.connector.write.WriterCommitMessage
import org.scalatest.matchers.should.Matchers
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.pinot.common.utils.TarCompressionUtils
import org.apache.pinot.spi.data.{FieldSpec, Schema}
import org.apache.pinot.spi.data.readers.GenericRow
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeArrayData, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.unsafe.types.UTF8String
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

  test("internalRowToGenericRow handles UnsafeArrayData and null fields") {
    // Regression test for the original bug: the writer called ArrayData.array() on every
    // ArrayType column. Spark's DataSourceV2 write path always feeds the writer an UnsafeRow
    // whose array fields are UnsafeArrayData, and UnsafeArrayData.array() throws
    // UnsupportedOperationException -- so the pre-fix writer crashed on the first row of any
    // job projecting an ArrayType column. This test materializes an UnsafeRow via
    // UnsafeProjection so the array fields are guaranteed to be UnsafeArrayData, then exercises
    // every supported element type plus a null array field and null scalar fields. Element-level
    // null handling is covered by the negative tests below.
    val writeOptions = PinotDataSourceWriteOptions(
      tableName = "testTable",
      savePath = "/tmp/pinot",
      timeColumnName = "ts",
      timeFormat = "EPOCH|SECONDS",
      timeGranularity = "1:SECONDS",
      segmentNameFormat = "{table}_{partitionId:03}",
      invertedIndexColumns = Array(),
      noDictionaryColumns = Array(),
      bloomFilterColumns = Array(),
      rangeIndexColumns = Array()
    )
    val writeSchema = StructType(Seq(
      StructField("ts", LongType, nullable = false),
      StructField("name", StringType, nullable = true),
      StructField("nullableInt", IntegerType, nullable = true),
      StructField("strs", ArrayType(StringType), nullable = true),
      StructField("ints", ArrayType(IntegerType), nullable = true),
      StructField("longs", ArrayType(LongType), nullable = true),
      StructField("floats", ArrayType(FloatType), nullable = true),
      StructField("doubles", ArrayType(DoubleType), nullable = true),
      StructField("bools", ArrayType(BooleanType), nullable = true),
      StructField("bytes", ArrayType(ByteType), nullable = true),
      StructField("shorts", ArrayType(ShortType), nullable = true),
      StructField("bins", ArrayType(BinaryType), nullable = true),
      StructField("nullArr", ArrayType(IntegerType), nullable = true)
    ))

    // Build the Pinot schema manually because SparkToPinotTypeTranslator does not yet map
    // ByteType / ShortType arrays, and the writer's write() path does not consult pinotSchema
    // (it is only used at commit time to drive segment generation).
    val pinotSchema = new Schema.SchemaBuilder()
      .setSchemaName(writeOptions.tableName)
      .addDateTimeField("ts", FieldSpec.DataType.LONG,
        writeOptions.timeFormat, writeOptions.timeGranularity)
      .build()
    val writer = new PinotDataWriter[InternalRow](0, 0, writeOptions, writeSchema, pinotSchema)

    val genericRow = new GenericInternalRow(Array[Any](
      1234567890L,
      null,
      null,
      new GenericArrayData(Array[Any](UTF8String.fromString("a"), UTF8String.fromString("b"))),
      new GenericArrayData(Array[Any](1, 2, 3)),
      new GenericArrayData(Array[Any](10L, 20L)),
      new GenericArrayData(Array[Any](1.5f, 2.5f)),
      new GenericArrayData(Array[Any](1.5d, 2.5d)),
      new GenericArrayData(Array[Any](true, false, true)),
      new GenericArrayData(Array[Any](1.toByte, 2.toByte)),
      new GenericArrayData(Array[Any](100.toShort, 200.toShort)),
      new GenericArrayData(Array[Any]("a".getBytes, "b".getBytes)),
      null
    ))
    val unsafeRow = UnsafeProjection.create(writeSchema).apply(genericRow)
    // Self-validate that the projection actually produced an UnsafeRow whose array fields are
    // UnsafeArrayData. Without this guard, a future change that accidentally substitutes a
    // GenericInternalRow / GenericArrayData would silently degrade this into a non-regression test.
    unsafeRow shouldBe an[UnsafeRow]
    unsafeRow.getArray(writeSchema.fieldIndex("ints")) shouldBe an[UnsafeArrayData]
    unsafeRow.getArray(writeSchema.fieldIndex("strs")) shouldBe an[UnsafeArrayData]

    writer.write(unsafeRow)
    val gr = writer.bufferedRecordReader.next()

    gr.getValue("ts") shouldBe 1234567890L
    gr.getValue("name") shouldBe null
    gr.getValue("nullableInt") shouldBe null
    gr.getValue("nullArr") shouldBe null

    // Pinot expects MV columns as Object[] (or arrays of references) -- see GenericRow.copy()
    // and the pinot-segment-local stats collectors which all cast to (Object[]).
    gr.getValue("strs").asInstanceOf[Array[String]].toSeq shouldBe Seq("a", "b")
    gr.getValue("ints").asInstanceOf[Array[AnyRef]].toSeq shouldBe Seq(Integer.valueOf(1),
      Integer.valueOf(2), Integer.valueOf(3))
    gr.getValue("longs").asInstanceOf[Array[AnyRef]].toSeq shouldBe Seq(java.lang.Long.valueOf(10L),
      java.lang.Long.valueOf(20L))
    gr.getValue("floats").asInstanceOf[Array[AnyRef]].toSeq shouldBe Seq(java.lang.Float.valueOf(1.5f),
      java.lang.Float.valueOf(2.5f))
    gr.getValue("doubles").asInstanceOf[Array[AnyRef]].toSeq shouldBe Seq(
      java.lang.Double.valueOf(1.5d), java.lang.Double.valueOf(2.5d))
    gr.getValue("bools").asInstanceOf[Array[AnyRef]].toSeq shouldBe Seq(java.lang.Boolean.TRUE,
      java.lang.Boolean.FALSE, java.lang.Boolean.TRUE)
    gr.getValue("bytes").asInstanceOf[Array[AnyRef]].toSeq shouldBe Seq(java.lang.Byte.valueOf(1.toByte),
      java.lang.Byte.valueOf(2.toByte))
    gr.getValue("shorts").asInstanceOf[Array[AnyRef]].toSeq shouldBe Seq(
      java.lang.Short.valueOf(100.toShort), java.lang.Short.valueOf(200.toShort))

    val bins = gr.getValue("bins").asInstanceOf[Array[Array[Byte]]]
    bins.length shouldBe 2
    bins(0).toSeq shouldBe "a".getBytes.toSeq
    bins(1).toSeq shouldBe "b".getBytes.toSeq

    writer.close()
  }

  test("internalRowToGenericRow rejects null elements in multi-value array columns") {
    // Pinot's MV ingestion pipeline does not support null elements -- the per-type stats
    // collectors in pinot-segment-local each cast `(Object[])` and unbox without a null check.
    // The patched writer must therefore fail fast with IllegalArgumentException rather than
    // either NPEing on UnsafeArrayData (pre-fix), silently emitting 0 (GenericArrayData), or
    // letting the segment build crash later with an opaque downstream NPE.
    val writeOptions = PinotDataSourceWriteOptions(
      tableName = "testTable",
      savePath = "/tmp/pinot",
      timeColumnName = "ts",
      timeFormat = "EPOCH|SECONDS",
      timeGranularity = "1:SECONDS",
      segmentNameFormat = "{table}_{partitionId:03}",
      invertedIndexColumns = Array(),
      noDictionaryColumns = Array(),
      bloomFilterColumns = Array(),
      rangeIndexColumns = Array()
    )
    val pinotSchema = new Schema.SchemaBuilder()
      .setSchemaName(writeOptions.tableName)
      .addDateTimeField("ts", FieldSpec.DataType.LONG,
        writeOptions.timeFormat, writeOptions.timeGranularity)
      .build()

    val cases = Seq(
      ("ints", IntegerType,
        new GenericArrayData(Array[Any](Integer.valueOf(1), null, Integer.valueOf(3)))),
      ("longs", LongType,
        new GenericArrayData(Array[Any](java.lang.Long.valueOf(1L), null))),
      ("floats", FloatType,
        new GenericArrayData(Array[Any](java.lang.Float.valueOf(1.0f), null))),
      ("doubles", DoubleType,
        new GenericArrayData(Array[Any](java.lang.Double.valueOf(1.0d), null))),
      ("bools", BooleanType,
        new GenericArrayData(Array[Any](java.lang.Boolean.TRUE, null))),
      ("bytes", ByteType,
        new GenericArrayData(Array[Any](java.lang.Byte.valueOf(1.toByte), null))),
      ("shorts", ShortType,
        new GenericArrayData(Array[Any](java.lang.Short.valueOf(1.toShort), null))),
      ("strs", StringType,
        new GenericArrayData(Array[Any](UTF8String.fromString("a"), null))),
      ("bins", BinaryType,
        new GenericArrayData(Array[Any]("a".getBytes, null)))
    )

    cases.foreach { case (columnName, elementType, arrayData) =>
      val writeSchema = StructType(Seq(
        StructField("ts", LongType, nullable = false),
        StructField(columnName, ArrayType(elementType, containsNull = true), nullable = true)
      ))
      val writer = new PinotDataWriter[InternalRow](0, 0, writeOptions, writeSchema, pinotSchema)
      val row = new GenericInternalRow(Array[Any](1L, arrayData))
      val unsafeRow = UnsafeProjection.create(writeSchema).apply(row)

      val ex = intercept[IllegalArgumentException] {
        writer.write(unsafeRow)
      }
      ex.getMessage should include(s"'$columnName'")
      ex.getMessage should include("null element")

      writer.close()
    }
  }

  test("commit produces a valid segment from an UnsafeRow with multi-value array columns") {
    // End-to-end regression: feed the writer an UnsafeRow shaped like what
    // DataSourceV2 actually delivers in production, drive commit() to run the full
    // SegmentIndexCreationDriverImpl pipeline, and assert the segment tarball lands.
    // Restricted to MV element types Pinot natively supports (INT, LONG, FLOAT, DOUBLE, STRING).
    tmpDir = Files.createTempDirectory("pinot-spark-connector-mv-commit-test").toFile

    val writeOptions = PinotDataSourceWriteOptions(
      tableName = "mvTable",
      savePath = tmpDir.getAbsolutePath,
      timeColumnName = "ts",
      timeFormat = "EPOCH|SECONDS",
      timeGranularity = "1:SECONDS",
      segmentNameFormat = "{table}_{startTime}_{endTime}_{partitionId:03}",
      invertedIndexColumns = Array(),
      noDictionaryColumns = Array(),
      bloomFilterColumns = Array(),
      rangeIndexColumns = Array()
    )
    val writeSchema = StructType(Seq(
      StructField("ts", LongType, nullable = false),
      StructField("name", StringType, nullable = false),
      StructField("ints", ArrayType(IntegerType), nullable = false),
      StructField("longs", ArrayType(LongType), nullable = false),
      StructField("floats", ArrayType(FloatType), nullable = false),
      StructField("doubles", ArrayType(DoubleType), nullable = false),
      StructField("strs", ArrayType(StringType), nullable = false)
    ))
    val pinotSchema = SparkToPinotTypeTranslator.translate(
      writeSchema, writeOptions.tableName, writeOptions.timeColumnName,
      writeOptions.timeFormat, writeOptions.timeGranularity)
    val writer = new PinotDataWriter[InternalRow](0, 0, writeOptions, writeSchema, pinotSchema)

    val unsafeProjection = UnsafeProjection.create(writeSchema)
    Seq(
      (1234567890L, "Alice", Array[Any](1, 2), Array[Any](10L, 20L),
        Array[Any](1.5f, 2.5f), Array[Any](1.5d, 2.5d),
        Array[Any](UTF8String.fromString("a"), UTF8String.fromString("b"))),
      (1234567891L, "Bob", Array[Any](3), Array[Any](30L),
        Array[Any](3.5f), Array[Any](3.5d),
        Array[Any](UTF8String.fromString("c")))
    ).foreach { case (ts, name, ints, longs, floats, doubles, strs) =>
      val row = new GenericInternalRow(Array[Any](
        ts, UTF8String.fromString(name),
        new GenericArrayData(ints), new GenericArrayData(longs),
        new GenericArrayData(floats), new GenericArrayData(doubles),
        new GenericArrayData(strs)
      ))
      writer.write(unsafeProjection.apply(row).copy())
    }

    val commitMessage = writer.commit()
    commitMessage shouldBe a[SuccessWriterCommitMessage]

    val fs = FileSystem.get(new URI(writeOptions.savePath), new org.apache.hadoop.conf.Configuration())
    val segmentPath = new Path(writeOptions.savePath + "/mvTable_1234567890_1234567891_000.tar.gz")
    fs.exists(segmentPath) shouldBe true

    TarCompressionUtils.untar(
      new File(writeOptions.savePath + "/mvTable_1234567890_1234567891_000.tar.gz"),
      new File(writeOptions.savePath))
    val untarDir = Paths.get(writeOptions.savePath + "/mvTable_1234567890_1234567891_000/v3/")
    Files.exists(untarDir) shouldBe true

    val metadataSrc = Source.fromFile(untarDir + "/metadata.properties")
    val metadataContent = metadataSrc.getLines.mkString("\n")
    metadataSrc.close()

    metadataContent should include("segment.name = mvTable_1234567890_1234567891_000")
    metadataContent should include("column.ints.dataType = INT")
    metadataContent should include("column.ints.isSingleValues = false")
    metadataContent should include("column.longs.dataType = LONG")
    metadataContent should include("column.longs.isSingleValues = false")
    metadataContent should include("column.floats.dataType = FLOAT")
    metadataContent should include("column.floats.isSingleValues = false")
    metadataContent should include("column.doubles.dataType = DOUBLE")
    metadataContent should include("column.doubles.isSingleValues = false")
    metadataContent should include("column.strs.dataType = STRING")
    metadataContent should include("column.strs.isSingleValues = false")
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

  // The production code reads strings via `getUTF8String(idx).toString` (the SpecializedGetters
  // contract); GenericInternalRow's default would `ClassCastException` on a plain Java String.
  override def getUTF8String(ordinal: Int): UTF8String =
    values(ordinal) match {
      case null => null
      case s: String => UTF8String.fromString(s)
      case u: UTF8String => u
    }
}
