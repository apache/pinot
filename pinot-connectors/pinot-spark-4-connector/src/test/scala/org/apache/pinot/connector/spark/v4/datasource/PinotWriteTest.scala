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
package org.apache.pinot.connector.spark.v4.datasource

import org.apache.pinot.spi.data.Schema
import org.apache.spark.sql.connector.write.LogicalWriteInfo
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._


class PinotWriteTest extends AnyFunSuite with Matchers {
  test("Constructor should initialize writeOptions, writeSchema, and pinotSchema correctly") {
    val options = Map(
      "table" -> "testTable",
      "segmentNameFormat" -> "my_segment_format",
      "path" -> "/path/to/save",
      "timeColumnName" -> "timeCol",
      "timeFormat" -> "EPOCH|SECONDS",
      "timeGranularity" -> "1:SECONDS",
      "invertedIndexColumns" -> "col1,col2",
      "noDictionaryColumns" -> "col3,col4",
      "bloomFilterColumns" -> "col5,col6",
      "rangeIndexColumns" -> "col7,col8"
    )

    val schema = StructType(Seq(
      StructField("col1", StringType),
      StructField("col2", StringType),
      StructField("timeCol", LongType),
    ))

    val logicalWriteInfo = new TestLogicalWriteInfo(new CaseInsensitiveStringMap(options.asJava), schema)

    val pinotWrite = new PinotWrite(logicalWriteInfo)

    pinotWrite.writeOptions.tableName shouldEqual "testTable"
    pinotWrite.writeOptions.segmentNameFormat shouldEqual "my_segment_format"
    pinotWrite.writeOptions.savePath shouldEqual "/path/to/save"
    pinotWrite.writeOptions.timeColumnName shouldEqual "timeCol"
    pinotWrite.writeOptions.timeFormat shouldEqual "EPOCH|SECONDS"
    pinotWrite.writeOptions.timeGranularity shouldEqual "1:SECONDS"
    pinotWrite.writeOptions.invertedIndexColumns shouldEqual Array("col1", "col2")
    pinotWrite.writeOptions.noDictionaryColumns shouldEqual Array("col3", "col4")
    pinotWrite.writeOptions.bloomFilterColumns shouldEqual Array("col5", "col6")
    pinotWrite.writeOptions.rangeIndexColumns shouldEqual Array("col7", "col8")

    pinotWrite.writeSchema shouldEqual schema

    val expectedPinotSchema = Schema.fromString(
      """
        |{
        |  "schemaName": "testTable",
        |  "dimensionFieldSpecs": [
        |    {"name": "col1", "dataType": "STRING"},
        |    {"name": "col2", "dataType": "STRING"}
        |  ],
        |  "dateTimeFieldSpecs" : [ {
        |    "name" : "timeCol",
        |    "dataType" : "LONG",
        |    "fieldType" : "DATE_TIME",
        |    "notNull" : false,
        |    "format" : "EPOCH|SECONDS",
        |    "granularity" : "1:SECONDS"
        |  } ]
        |}
        |""".stripMargin)
    pinotWrite.pinotSchema shouldEqual expectedPinotSchema
  }

  test("PinotWriteBuilder.overwrite(...) rejects predicates instead of silently appending") {
    // Spark 4's SupportsOverwriteV2 contract is that matching rows are replaced; Pinot's
    // write path only appends. Advertising but silently ignoring overwrite semantics would
    // leave stale rows in place. Fail fast with a clear message instead.
    import org.apache.spark.sql.connector.expressions.filter.{AlwaysTrue, Predicate}
    val info = new TestLogicalWriteInfo(
      new CaseInsensitiveStringMap(Map.empty[String, String].asJava),
      StructType(Seq(StructField("id", LongType))))
    val builder = new PinotWriteBuilder(info)

    val ex = intercept[UnsupportedOperationException] {
      builder.overwrite(Array[Predicate](new AlwaysTrue()))
    }
    ex.getMessage should include("does not support overwrite")
    ex.getMessage should include("1")
  }

  test("PinotWriteBuilder.truncate() rejects silent overwrite for df.write.mode(\"overwrite\")") {
    // SupportsOverwriteV2 extends SupportsTruncate; Spark 4's V2Writes analyzer dispatches
    // df.write.mode("overwrite") → truncate() rather than overwrite([AlwaysTrue]). Without
    // an explicit override, truncate() returns `this` and build() silently appends — the
    // very bug the overwrite() rejection is meant to prevent. This test pins the override
    // so the truncate path fails fast too.
    val info = new TestLogicalWriteInfo(
      new CaseInsensitiveStringMap(Map.empty[String, String].asJava),
      StructType(Seq(StructField("id", LongType))))
    val builder = new PinotWriteBuilder(info)

    val ex = intercept[UnsupportedOperationException] {
      builder.truncate()
    }
    ex.getMessage should include("does not support truncate / overwrite")
    ex.getMessage should include("df.write.mode(\"append\")")
  }

  // -------- abort() runtime cleanup tests --------
  // These cover the new best-effort leftover-tar deletion path that runs on the driver when
  // a write job aborts (one or more tasks failed). The branches exercised are: missing
  // savePath, malformed scheme (FileSystem.get throws), the happy path (delete succeeds for
  // SuccessWriterCommitMessage), null entries from Spark, and unknown subclasses.

  import java.io.File
  import java.nio.file.{Files, Paths}
  import org.apache.pinot.spi.ingestion.batch.spec.Constants
  import org.apache.spark.sql.connector.write.WriterCommitMessage

  private def writeWithSavePath(savePath: String): PinotWrite = {
    val options = Map(
      "table" -> "abortTest",
      "segmentNameFormat" -> "my_segment",
      "path" -> savePath,
      "timeColumnName" -> "ts",
      "timeFormat" -> "EPOCH|SECONDS",
      "timeGranularity" -> "1:SECONDS"
    )
    val schema = StructType(Seq(StructField("id", LongType), StructField("ts", LongType)))
    new PinotWrite(new TestLogicalWriteInfo(
      new CaseInsensitiveStringMap(options.asJava), schema))
  }

  test("PinotWrite.abort() does not throw when savePath is empty") {
    // PinotDataSourceWriteOptions.from() requires a non-null path, but an empty string
    // bypasses that check. abort() must short-circuit on empty/null savePath without
    // attempting any FileSystem call.
    val pinotWrite = writeWithSavePath("")
    noException should be thrownBy pinotWrite.abort(
      Array(new SuccessWriterCommitMessage("seg1")))
  }

  test("PinotWrite.abort() tolerates a malformed savePath URI") {
    // Some FileSystem.get(URI, conf) implementations throw on unrecognized schemes.
    // abort() must catch and log without rethrowing — the driver already has a failure
    // to surface and a cleanup miss is recoverable.
    val pinotWrite = writeWithSavePath("not-a-real-scheme://nowhere/segments")
    noException should be thrownBy pinotWrite.abort(
      Array(new SuccessWriterCommitMessage("seg1")))
  }

  test("PinotWrite.abort() deletes leftover tars for SuccessWriterCommitMessage entries") {
    val tmp = Files.createTempDirectory("pinot-spark4-abort-test").toFile
    try {
      val seg1Tar = new File(tmp, "seg1" + Constants.TAR_GZ_FILE_EXT)
      val seg2Tar = new File(tmp, "seg2" + Constants.TAR_GZ_FILE_EXT)
      Files.createFile(seg1Tar.toPath)
      Files.createFile(seg2Tar.toPath)
      seg1Tar.exists() shouldBe true
      seg2Tar.exists() shouldBe true

      val pinotWrite = writeWithSavePath("file://" + tmp.getAbsolutePath)
      pinotWrite.abort(Array(
        new SuccessWriterCommitMessage("seg1"),
        new SuccessWriterCommitMessage("seg2")))

      seg1Tar.exists() shouldBe false
      seg2Tar.exists() shouldBe false
    } finally {
      org.apache.commons.io.FileUtils.deleteQuietly(tmp)
    }
  }

  test("PinotWrite.abort() tolerates null and unknown commit message types") {
    val tmp = Files.createTempDirectory("pinot-spark4-abort-test").toFile
    try {
      val segTar = new File(tmp, "real-seg" + Constants.TAR_GZ_FILE_EXT)
      Files.createFile(segTar.toPath)

      val pinotWrite = writeWithSavePath("file://" + tmp.getAbsolutePath)
      val unknown = new WriterCommitMessage {} // anonymous subclass, not SuccessWriterCommitMessage
      noException should be thrownBy pinotWrite.abort(Array(
        null,
        unknown,
        new SuccessWriterCommitMessage("real-seg")))
      // The Success entry should still have been cleaned up alongside the harmless null /
      // unknown entries.
      segTar.exists() shouldBe false
    } finally {
      org.apache.commons.io.FileUtils.deleteQuietly(tmp)
    }
  }
}

class TestLogicalWriteInfo(
                            options: CaseInsensitiveStringMap,
                            schema: StructType,
                            queryId: String = "testQueryId"
                          ) extends LogicalWriteInfo {
  override def options(): CaseInsensitiveStringMap = options
  override def schema(): StructType = schema
  override def queryId(): String = queryId
}
