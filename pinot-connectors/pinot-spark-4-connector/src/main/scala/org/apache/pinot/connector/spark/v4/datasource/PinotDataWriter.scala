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

import org.apache.commons.io.FileUtils
import org.apache.pinot.common.utils.TarCompressionUtils
import org.apache.pinot.connector.spark.common.PinotDataSourceWriteOptions
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig
import org.apache.pinot.spi.config.table.{IndexingConfig, SegmentsValidationAndRetentionConfig, TableConfig, TableCustomConfig, TenantConfig}
import org.apache.pinot.spi.data.readers.GenericRow
import org.apache.pinot.spi.data.Schema
import org.apache.pinot.spi.ingestion.batch.spec.Constants
import org.apache.pinot.spi.utils.DataSizeUtils
import org.apache.spark.sql.catalyst
import org.apache.spark.sql.types.StructType
import org.slf4j.{Logger, LoggerFactory}

import java.io.File
import java.nio.file.Files
import java.util.regex.Pattern

/** This class implements the Spark DataWriter interface for Pinot and is executed on the Spark executor side.
 *  It takes in Spark records through the `write` method and creates and pushes a segment to the destination on `commit`.
 *
 *  In current design, records are written to a buffered record reader, which adheres to the Pinot RecordReader interface.
 *  The reason for this is the Pinot SegmentIndexCreationDriverImpl expects a RecordReader and does two passes over the data:
 *  one for statistics generation and one for actual segment creation.
 *
 *  Segment name generation is also executed here in order to make it possible for segment names which derive some
 *  parts from actual data (records), such as minTimestamp, maxTimestamp, etc.
 *
 *  Thread safety: NOT thread-safe. Per the Spark DataWriter contract, each instance is owned by
 *  a single Spark task and the framework invokes write/commit/abort/close sequentially from the
 *  task thread. Mutable state (`segmentOutputDir`, `bufferedRecordReader`, `startTime`,
 *  `endTime`) therefore needs no synchronization. `commit()` and `abort()` are mutually
 *  exclusive (Spark calls one or the other, not both); `close()` may be called after either
 *  and is idempotent w.r.t. temp-dir cleanup and reader close.
 */
class PinotDataWriter[InternalRow](
                                    partitionId: Int,
                                    taskId: Long,
                                    writeOptions: PinotDataSourceWriteOptions,
                                    writeSchema: StructType,
                                    pinotSchema: Schema)
  extends DataWriter[org.apache.spark.sql.catalyst.InternalRow] with AutoCloseable {
  private val logger: Logger = LoggerFactory.getLogger(classOf[PinotDataWriter[InternalRow]])
  logger.info("PinotDataWriter created with writeOptions: {}, partitionId: {}, taskId: {}",
    (writeOptions, partitionId, taskId))

  private[pinot] val tableName: String = writeOptions.tableName
  private[pinot] val savePath: String = writeOptions.savePath
  private[pinot] val bufferedRecordReader: PinotBufferedRecordReader = new PinotBufferedRecordReader()

  private val timeColumnName = writeOptions.timeColumnName
  private val timeColumnIndex = if (timeColumnName != null) writeSchema.fieldIndex(timeColumnName) else -1

  // Holds the per-partition temp directory that `generateSegment` creates so that `commit()`
  // and `abort()` can delete it. Without this, long-running executors accumulate uncompressed
  // segment build artifacts under the JVM tmpdir and can fill the disk.
  private var segmentOutputDir: File = _

  // Tracks whether the writer's close-once resources (buffered reader, temp dir tracker) have
  // already been released, so close() after abort() is a no-op rather than relying on
  // PinotBufferedRecordReader.close() being idempotent. See the class-level Javadoc for the
  // commit/abort/close ordering contract.
  private var closed = false

  private var isTimeColumnNumeric = false
  if (timeColumnIndex > -1) {
    isTimeColumnNumeric = writeSchema.fields(timeColumnIndex).dataType match {
      case org.apache.spark.sql.types.IntegerType => true
      case org.apache.spark.sql.types.LongType => true
      case _ => false
    }
  }
  private var startTime = Long.MaxValue
  private var endTime = 0L

  override def write(record: catalyst.InternalRow): Unit = {
    bufferedRecordReader.write(internalRowToGenericRow(record))

    // Tracking startTime and endTime for segment name generation purposes
    if (timeColumnIndex > -1 && isTimeColumnNumeric) {
      val time = record.getLong(timeColumnIndex)
      startTime = Math.min(startTime, time)
      endTime = Math.max(endTime, time)
    }
  }

  override def commit(): WriterCommitMessage = {
    val segmentName = getSegmentName
    val segmentDir = generateSegment(segmentName)
    try {
      val segmentTarFile = tarSegmentDir(segmentName, segmentDir)
      pushSegmentTarFile(segmentTarFile)
      new SuccessWriterCommitMessage(segmentName)
    } finally {
      // Always clear the temp build dir — both the uncompressed segment tree and the tar file
      // live inside it, and once the tar is copied to savePath the local copies are disposable.
      FileUtils.deleteQuietly(segmentDir)
      segmentOutputDir = null
    }
  }

  /** This method is used to generate the segment name based on the format
   *  provided in the write options (segmentNameFormat).
   *  The format can contain variables like {partitionId}.
   *  Currently supported variables are `partitionId`, `table`, `startTime` and `endTime`
   *
   *  `startTime` and `endTime` are the minimum and maximum values of the time column in the records
   *  and it is only available if the time column is numeric.
   *
   *  It also supports the following, python inspired format specifier for digit formatting:
   *  `{partitionId:05}`
   *  which will zero pad partitionId up to five characters.
   *
   *  Some examples:
   *    "{partitionId}_{table}" -> "12_airlineStats"
   *    "{partitionId:05}_{table}" -> "00012_airlineStats"
   *    "{table}_{partitionId}" -> "airlineStats_12"
   *    "{table}_20240805" -> "airlineStats_20240805"
   *    "{table}_{startTime}_{endTime}_{partitionId:03}" -> "airlineStats_1234567890_1234567891_012"
   */
  private[pinot] def getSegmentName: String = {
    val format = writeOptions.segmentNameFormat
    val variables = Map(
      "partitionId" -> partitionId,
      "table" -> tableName,
      "startTime" -> startTime,
      "endTime" -> endTime,
    )

    val pattern = Pattern.compile("\\{(\\w+)(?::(\\d+))?}")
    val matcher = pattern.matcher(format)

    val buffer = new StringBuffer()
    while (matcher.find()) {
      val variableName = matcher.group(1)
      val formatSpecifier = matcher.group(2)
      val value = variables(variableName)

      val formattedValue = formatSpecifier match {
        case null => value.toString
        // Width spec is only well-defined on numeric variables (`partitionId`, `startTime`,
        // `endTime`) where it zero-pads the digit count. On a non-numeric variable like
        // `{table}`, %Ns would emit leading whitespace inside the segment name (path-hostile
        // for Hadoop FS, controller URLs, listings) and %-Ns would emit trailing whitespace —
        // neither matches the documented analogue `{partitionId:05}`. Reject early with a
        // clear message so the user sees the issue at job submission rather than at commit.
        case spec => value match {
          case n: Number => String.format(s"%${spec}d", n)
          case _ =>
            throw new IllegalArgumentException(
              s"segmentNameFormat width spec ':$spec' is only supported on numeric variables " +
                s"(partitionId, startTime, endTime); '$variableName' is not numeric. " +
                s"Use '{$variableName}' without a width spec.")
        }
      }

      matcher.appendReplacement(buffer, formattedValue)
    }
    matcher.appendTail(buffer)

    buffer.toString
  }

  private[pinot] def generateSegment(segmentName: String): File = {
    val outputDir = Files.createTempDirectory(classOf[PinotDataWriter[InternalRow]].getName).toFile
    // Record for cleanup in abort(); commit()'s finally block overrides this to null after
    // cleaning up itself.
    segmentOutputDir = outputDir
    val indexingConfig = getIndexConfig
    val segmentGeneratorConfig = getSegmentGenerationConfig(segmentName, indexingConfig, outputDir)

    logger.info("Creating segment with indexConfig: {} and segmentGeneratorConfig config: {}",
      (indexingConfig, segmentGeneratorConfig))

    // create segment and return output directory
    val driver = new SegmentIndexCreationDriverImpl()
    driver.init(segmentGeneratorConfig, bufferedRecordReader)
    driver.build()
    outputDir
  }

  private def getIndexConfig: IndexingConfig = {
    val indexingConfig = new IndexingConfig
    indexingConfig.setInvertedIndexColumns(java.util.Arrays.asList(writeOptions.invertedIndexColumns:_*))
    indexingConfig.setNoDictionaryColumns(java.util.Arrays.asList(writeOptions.noDictionaryColumns:_*))
    indexingConfig.setBloomFilterColumns(java.util.Arrays.asList(writeOptions.bloomFilterColumns:_*))
    indexingConfig.setRangeIndexColumns(java.util.Arrays.asList(writeOptions.rangeIndexColumns:_*))
    indexingConfig
  }

  private def getSegmentGenerationConfig(segmentName: String,
                                         indexingConfig: IndexingConfig,
                                         outputDir: File,
                                        ): SegmentGeneratorConfig = {
    val segmentsValidationAndRetentionConfig = new SegmentsValidationAndRetentionConfig()
    segmentsValidationAndRetentionConfig.setTimeColumnName(timeColumnName)

    // Mostly dummy tableConfig, sufficient for segment generation purposes
    val tableConfig = new TableConfig(
      tableName,
      "OFFLINE",
      segmentsValidationAndRetentionConfig,
      new TenantConfig(null, null, null),
      indexingConfig,
      new TableCustomConfig(null),
      null, null, null, null, null, null, null,
      null, null, null, null, false, null, null,
      null, null)

    val segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, pinotSchema)
    segmentGeneratorConfig.setTableName(tableName)
    segmentGeneratorConfig.setSegmentName(segmentName)
    segmentGeneratorConfig.setOutDir(outputDir.getAbsolutePath)
    segmentGeneratorConfig
  }

  private def pushSegmentTarFile(segmentTarFile: File): Unit = {
    // TODO Support file systems other than local and HDFS
    val fs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(savePath), new org.apache.hadoop.conf.Configuration())
    val destPath = new org.apache.hadoop.fs.Path(savePath + "/" + segmentTarFile.getName)
    fs.copyFromLocalFile(new org.apache.hadoop.fs.Path(segmentTarFile.getAbsolutePath), destPath)

    logger.info("Pushed segment tar file {} to: {}", (segmentTarFile.getName, destPath))
  }

  private def internalRowToGenericRow(record: catalyst.InternalRow): GenericRow = {
    val gr = new GenericRow()

    writeSchema.fields.zipWithIndex foreach { case(field, idx) =>
      // Honor `record.isNullAt(idx)` before calling any typed accessor: Spark's primitive
      // accessors (`getInt`, `getLong`, etc.) silently return zero values for null cells,
      // which would corrupt the segment with synthetic zeros; `getString`/`getDecimal`
      // would NPE. Mark the field null on the GenericRow so the segment driver can apply
      // the column's defaultNullValue per Pinot's null-handling contract.
      if (record.isNullAt(idx)) {
        gr.putValue(field.name, null)
        gr.addNullValueField(field.name)
      } else field.dataType match {
        case org.apache.spark.sql.types.StringType =>
          gr.putValue(field.name, record.getString(idx))
        case org.apache.spark.sql.types.IntegerType =>
          gr.putValue(field.name, record.getInt(idx))
        case org.apache.spark.sql.types.LongType =>
          gr.putValue(field.name, record.getLong(idx))
        case org.apache.spark.sql.types.FloatType =>
          gr.putValue(field.name, record.getFloat(idx))
        case org.apache.spark.sql.types.DoubleType =>
          gr.putValue(field.name, record.getDouble(idx))
        case org.apache.spark.sql.types.BooleanType =>
          gr.putValue(field.name, record.getBoolean(idx))
        case org.apache.spark.sql.types.ByteType =>
          gr.putValue(field.name, record.getByte(idx))
        case org.apache.spark.sql.types.BinaryType =>
          gr.putValue(field.name, record.getBinary(idx))
        case org.apache.spark.sql.types.ShortType =>
          gr.putValue(field.name, record.getShort(idx))
        case dt: org.apache.spark.sql.types.DecimalType =>
          gr.putValue(field.name, record.getDecimal(idx, dt.precision, dt.scale).toJavaBigDecimal)
        case org.apache.spark.sql.types.ArrayType(elementType, _) =>
          // Use ArrayData's type-specific accessors instead of `.array`, which only works on
          // GenericArrayData (test scaffolding) and throws on the UnsafeArrayData that Spark
          // uses in real workloads. The previous `.array.map(_.asInstanceOf[T])` would
          // ClassCastException on real workloads, with a particularly bad failure for
          // StringType (UTF8String → String cast).
          val arrayData = record.getArray(idx)
          elementType match {
            case org.apache.spark.sql.types.StringType =>
              // ArrayData stores StringType elements as UTF8String, not Java String.
              val n = arrayData.numElements()
              val out = new Array[String](n)
              var i = 0
              while (i < n) {
                val s = arrayData.getUTF8String(i)
                out(i) = if (s == null) null else s.toString
                i += 1
              }
              gr.putValue(field.name, out)
            case org.apache.spark.sql.types.IntegerType =>
              gr.putValue(field.name, arrayData.toIntArray())
            case org.apache.spark.sql.types.LongType =>
              gr.putValue(field.name, arrayData.toLongArray())
            case org.apache.spark.sql.types.FloatType =>
              gr.putValue(field.name, arrayData.toFloatArray())
            case org.apache.spark.sql.types.DoubleType =>
              gr.putValue(field.name, arrayData.toDoubleArray())
            case org.apache.spark.sql.types.BooleanType =>
              gr.putValue(field.name, arrayData.toBooleanArray())
            case org.apache.spark.sql.types.ByteType =>
              gr.putValue(field.name, arrayData.toByteArray())
            case org.apache.spark.sql.types.ShortType =>
              gr.putValue(field.name, arrayData.toShortArray())
            case _ =>
              throw new UnsupportedOperationException(s"Unsupported array element type: $elementType")
          }
        case _ =>
          throw new UnsupportedOperationException("Unsupported data type: " + field.dataType)
      }
    }
    gr
  }

  private def tarSegmentDir(segmentName: String, segmentDir: File): File = {
    val localSegmentDir = new File(segmentDir, segmentName)
    val tarFile = new File(segmentDir, s"$segmentName${Constants.TAR_GZ_FILE_EXT}")
    logger.info("Local segment dir: {}", localSegmentDir.getAbsolutePath)

    TarCompressionUtils.createCompressedTarFile(localSegmentDir, tarFile)
    val uncompressedSegmentSize = FileUtils.sizeOf(localSegmentDir)
    val compressedSegmentSize = FileUtils.sizeOf(tarFile)

    logger.info("Size for segment: {}, uncompressed: {}, compressed: {}",
      segmentName,
      DataSizeUtils.fromBytes(uncompressedSegmentSize),
      DataSizeUtils.fromBytes(compressedSegmentSize))
    tarFile
  }

  override def abort(): Unit = {
    logger.info("Aborting writer")
    closeOnce()
  }

  override def close(): Unit = {
    logger.info("Closing writer")
    closeOnce()
  }

  // Spark calls abort() and then close() on some error paths. Use a closed flag instead of
  // relying on PinotBufferedRecordReader.close() being idempotent (it currently is, but the
  // contract is not declared; pinning it here avoids future-binding the invariant).
  // Wrap the reader close in try/finally so the temp-dir cleanup still runs if a future
  // PinotBufferedRecordReader.close() implementation throws — otherwise the closed flag
  // would already be set and the segmentOutputDir would leak on disk.
  private def closeOnce(): Unit = {
    if (closed) return
    closed = true
    try {
      bufferedRecordReader.close()
    } finally {
      if (segmentOutputDir != null) {
        FileUtils.deleteQuietly(segmentOutputDir)
        segmentOutputDir = null
      }
    }
  }
}

class SuccessWriterCommitMessage(val segmentName: String) extends WriterCommitMessage {
  override def toString: String = {
    "SuccessWriterCommitMessage{" +
      "segmentName='" + segmentName + '\'' +
      '}'
  }
}

