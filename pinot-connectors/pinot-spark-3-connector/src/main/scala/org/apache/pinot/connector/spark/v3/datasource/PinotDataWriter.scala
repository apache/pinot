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
import org.apache.pinot.common.utils.TarGzCompressionUtils
import org.apache.pinot.connector.spark.common.PinotDataSourceWriteOptions
import org.apache.spark.sql.connector.write.{DataWriter, PhysicalWriteInfo, WriterCommitMessage}
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

  val tableName = writeOptions.tableName
  val savePath = writeOptions.savePath
  val bufferedRecordReader = new PinotBufferedRecordReader()

  override def write(record: catalyst.InternalRow): Unit = {
    bufferedRecordReader.write(internalRowToGenericRow(record))
  }

  override def commit(): WriterCommitMessage = {
    val segmentName = getSegmentName
    val segmentDir = generateSegment(segmentName)
    val segmentTarFile = tarSegmentDir(segmentName, segmentDir)
    pushSegmentTarFile(segmentTarFile)
    new SuccessWriterCommitMessage(segmentName)
  }

  private[pinot] def getSegmentName: String = {
    writeOptions.segmentFormat.format(partitionId)
  }

  private[pinot] def generateSegment(segmentName: String): File = {
    val outputDir = Files.createTempDirectory(classOf[PinotDataWriter[InternalRow]].getName).toFile
    val indexingConfig = getIndexConfig

    logger.info("Index config: {}", indexingConfig)

    val segmentGeneratorConfig = getSegmentGenerationConfig(segmentName, indexingConfig, outputDir)

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
    // Mostly dummy tableConfig, sufficient for segment generation purposes
    val tableConfig = new TableConfig(
      tableName,
      "OFFLINE",
      new SegmentsValidationAndRetentionConfig(),
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

    logger.info("Pushed segment tar file to: {}", destPath)
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
            case org.apache.spark.sql.types.ShortType =>
              gr.putValue(field.name, record.getArray(idx).array.map(_.asInstanceOf[Short]))
            case _ =>
              throw new UnsupportedOperationException("Unsupported data type")
          }
        case _ =>
          throw new UnsupportedOperationException("Unsupported data type")
      }
    }
    gr
  }

  private def tarSegmentDir(segmentName: String, segmentDir: File): File = {
    val localSegmentDir = new File(segmentDir, segmentName)
    val tarFile = new File(segmentDir, s"$segmentName${Constants.TAR_GZ_FILE_EXT}")
    logger.info("Local segment dir: {}", localSegmentDir.getAbsolutePath)

    TarGzCompressionUtils.createTarGzFile(localSegmentDir, tarFile)
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

