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
      null)

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
      field.dataType match {
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
        case org.apache.spark.sql.types.ArrayType(elementType, _) =>
          elementType match {
            case org.apache.spark.sql.types.StringType =>
              gr.putValue(field.name, record.getArray(idx).array.map(_.asInstanceOf[String]))
            case org.apache.spark.sql.types.IntegerType =>
              gr.putValue(field.name, record.getArray(idx).array.map(_.asInstanceOf[Int]))
            case org.apache.spark.sql.types.LongType =>
              gr.putValue(field.name, record.getArray(idx).array.map(_.asInstanceOf[Long]))
            case org.apache.spark.sql.types.FloatType =>
              gr.putValue(field.name, record.getArray(idx).array.map(_.asInstanceOf[Float]))
            case org.apache.spark.sql.types.DoubleType =>
              gr.putValue(field.name, record.getArray(idx).array.map(_.asInstanceOf[Double]))
            case org.apache.spark.sql.types.BooleanType =>
              gr.putValue(field.name, record.getArray(idx).array.map(_.asInstanceOf[Boolean]))
            case org.apache.spark.sql.types.ByteType =>
              gr.putValue(field.name, record.getArray(idx).array.map(_.asInstanceOf[Byte]))
            case org.apache.spark.sql.types.BinaryType =>
              gr.putValue(field.name, record.getArray(idx).array.map(_.asInstanceOf[Array[Byte]]))
            case org.apache.spark.sql.types.ShortType =>
              gr.putValue(field.name, record.getArray(idx).array.map(_.asInstanceOf[Short]))
            case _ =>
              throw new UnsupportedOperationException(s"Unsupported data type: Array[${elementType}]")
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

