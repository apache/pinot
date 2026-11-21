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
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, ByteType, DataType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructType}
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

    // Tracking startTime and endTime for segment name generation purposes. Skip null values so a
    // null time column does not silently pull `startTime` to 0 and produce a malformed segment
    // name -- `record.getLong` returns 0 for nulls without an `isNullAt` guard.
    if (timeColumnIndex > -1 && isTimeColumnNumeric && !record.isNullAt(timeColumnIndex)) {
      val time = record.getLong(timeColumnIndex)
      startTime = Math.min(startTime, time)
      endTime = Math.max(endTime, time)
    }
  }

  override def commit(): WriterCommitMessage = {
    val segmentName = getSegmentName
    val segmentDir = generateSegment(segmentName)
    val segmentTarFile = tarSegmentDir(segmentName, segmentDir)
    pushSegmentTarFile(segmentTarFile)
    new SuccessWriterCommitMessage(segmentName)
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
        case spec => String.format(s"%${spec}d", value.asInstanceOf[Number])
      }

      matcher.appendReplacement(buffer, formattedValue)
    }
    matcher.appendTail(buffer)

    buffer.toString
  }

  private[pinot] def generateSegment(segmentName: String): File = {
    val outputDir = Files.createTempDirectory(classOf[PinotDataWriter[InternalRow]].getName).toFile
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

  /** Converts a Spark [[catalyst.InternalRow]] to a Pinot [[GenericRow]].
   *
   *  Spark's DataSourceV2 write path applies an `UnsafeProjection` immediately before invoking
   *  `DataWriter.write(...)`, so every row reaching this method is an `UnsafeRow` whose array
   *  fields are `UnsafeArrayData`. `UnsafeArrayData.array()` throws `UnsupportedOperationException`,
   *  so the array branch must use accessors that work on both `UnsafeArrayData` and
   *  `GenericArrayData` -- per-element iteration over the typed `getXxx(i)` methods.
   *
   *  Multi-value array fields are emitted as `Object[]` (boxed primitives, `String`s, or
   *  `byte[]`s) because Pinot's segment-generation pipeline expects this shape. Stats collectors
   *  in `pinot-segment-local` cast every MV entry as `(Object[])`, and `GenericRow.copy()`
   *  clones array values via `(Object[]) value` -- a primitive Spark array (`int[]`, `long[]`,
   *  ...) returned from `ArrayData.toIntArray()` / `toLongArray()` would `ClassCastException` at
   *  the first stats pass and never produce a segment.
   *
   *  Nullability is handled at the field level: a top-level `isNullAt` guard funnels every null
   *  field through `putValue(name, null)` so the scalar `StringType` branch never NPEs on a null
   *  `getUTF8String(...)` and primitive branches never silently substitute `0` / `false` for a
   *  null field value. Element-level nulls within a multi-value array are rejected with
   *  `IllegalArgumentException`: Pinot's MV stats collectors NPE on null elements, so failing
   *  fast in the writer with a clear message is strictly better than producing a corrupt
   *  segment or surfacing an opaque downstream crash.
   */
  private def internalRowToGenericRow(record: catalyst.InternalRow): GenericRow = {
    val gr = new GenericRow()

    writeSchema.fields.zipWithIndex foreach { case (field, idx) =>
      if (record.isNullAt(idx)) {
        gr.putValue(field.name, null)
      } else {
        field.dataType match {
          case StringType =>
            gr.putValue(field.name, record.getUTF8String(idx).toString)
          case IntegerType =>
            gr.putValue(field.name, record.getInt(idx))
          case LongType =>
            gr.putValue(field.name, record.getLong(idx))
          case FloatType =>
            gr.putValue(field.name, record.getFloat(idx))
          case DoubleType =>
            gr.putValue(field.name, record.getDouble(idx))
          case BooleanType =>
            gr.putValue(field.name, record.getBoolean(idx))
          case ByteType =>
            gr.putValue(field.name, record.getByte(idx))
          case BinaryType =>
            gr.putValue(field.name, record.getBinary(idx))
          case ShortType =>
            gr.putValue(field.name, record.getShort(idx))
          case ArrayType(elementType, _) =>
            gr.putValue(field.name, convertArrayData(record.getArray(idx), field.name, elementType))
          case _ =>
            throw new UnsupportedOperationException("Unsupported data type: " + field.dataType)
        }
      }
    }
    gr
  }

  private def convertArrayData(arr: ArrayData, columnName: String, elementType: DataType): AnyRef = {
    val n = arr.numElements()
    elementType match {
      case StringType =>
        val out = new Array[String](n)
        var i = 0
        while (i < n) {
          requireNoNullElement(arr, columnName, i)
          out(i) = arr.getUTF8String(i).toString
          i += 1
        }
        out
      case BinaryType =>
        val out = new Array[Array[Byte]](n)
        var i = 0
        while (i < n) {
          requireNoNullElement(arr, columnName, i)
          out(i) = arr.getBinary(i)
          i += 1
        }
        out
      case IntegerType =>
        val out = new Array[AnyRef](n)
        var i = 0
        while (i < n) {
          requireNoNullElement(arr, columnName, i)
          out(i) = Integer.valueOf(arr.getInt(i))
          i += 1
        }
        out
      case LongType =>
        val out = new Array[AnyRef](n)
        var i = 0
        while (i < n) {
          requireNoNullElement(arr, columnName, i)
          out(i) = java.lang.Long.valueOf(arr.getLong(i))
          i += 1
        }
        out
      case FloatType =>
        val out = new Array[AnyRef](n)
        var i = 0
        while (i < n) {
          requireNoNullElement(arr, columnName, i)
          out(i) = java.lang.Float.valueOf(arr.getFloat(i))
          i += 1
        }
        out
      case DoubleType =>
        val out = new Array[AnyRef](n)
        var i = 0
        while (i < n) {
          requireNoNullElement(arr, columnName, i)
          out(i) = java.lang.Double.valueOf(arr.getDouble(i))
          i += 1
        }
        out
      case BooleanType =>
        val out = new Array[AnyRef](n)
        var i = 0
        while (i < n) {
          requireNoNullElement(arr, columnName, i)
          out(i) = java.lang.Boolean.valueOf(arr.getBoolean(i))
          i += 1
        }
        out
      case ByteType =>
        val out = new Array[AnyRef](n)
        var i = 0
        while (i < n) {
          requireNoNullElement(arr, columnName, i)
          out(i) = java.lang.Byte.valueOf(arr.getByte(i))
          i += 1
        }
        out
      case ShortType =>
        val out = new Array[AnyRef](n)
        var i = 0
        while (i < n) {
          requireNoNullElement(arr, columnName, i)
          out(i) = java.lang.Short.valueOf(arr.getShort(i))
          i += 1
        }
        out
      case _ =>
        throw new UnsupportedOperationException(s"Unsupported data type: Array[$elementType]")
    }
  }

  private def requireNoNullElement(arr: ArrayData, columnName: String, idx: Int): Unit = {
    if (arr.isNullAt(idx)) {
      throw new IllegalArgumentException(
        s"Multi-value column '$columnName' contains a null element at index $idx. Pinot " +
          "multi-value columns do not support null elements; filter or coalesce nulls in Spark " +
          "before writing.")
    }
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
    bufferedRecordReader.close()
  }

  override def close(): Unit = {
    logger.info("Closing writer")
    bufferedRecordReader.close()
  }
}

class SuccessWriterCommitMessage(segmentName: String) extends WriterCommitMessage {
  override def toString: String = {
    "SuccessWriterCommitMessage{" +
      "segmentName='" + segmentName + '\'' +
      '}'
  }
}

