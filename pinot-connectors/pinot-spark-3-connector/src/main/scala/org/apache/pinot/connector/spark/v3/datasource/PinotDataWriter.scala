package org.apache.pinot.connector.spark.v3.datasource

import org.apache.commons.io.FileUtils
import org.apache.pinot.common.utils.TarGzCompressionUtils
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

import java.io.File
import java.nio.file.Files

class PinotDataWriter[InternalRow](
                                    physicalWriteInfo: PhysicalWriteInfo,
                                    partitionId: Int,
                                    taskId: Long,
                                    tableName: String,
                                    segmentNameFormat: String,
                                    savePath: String,
                                    writeSchema: StructType,
                                    pinotSchema: Schema,
                                  )
  extends DataWriter[org.apache.spark.sql.catalyst.InternalRow] with AutoCloseable {

  println("PinotDataWriter created with " +
    "physicalWriteInfo: " + physicalWriteInfo +
    ", partitionId: " + partitionId + ", taskId: " + taskId)

  private val bufferedRecordReader = new PinotBufferedRecordReader()
  private var tableConfig: TableConfig = null
  private var segmentGeneratorConfig: SegmentGeneratorConfig = null
  private var localOutputTempDir: File = null

  override def write(record: catalyst.InternalRow): Unit = {
    bufferedRecordReader.write(internalRowToGenericRow(record))
  }

  override def commit(): WriterCommitMessage = {
    initSegmentCreator()

    val driver = new SegmentIndexCreationDriverImpl()
    driver.init(segmentGeneratorConfig, bufferedRecordReader)
    driver.build()

    // tar segment
    val segmentTarFile = tarSegmentDir(segmentGeneratorConfig.getSegmentName)
    printf("Segment tar file: %s\n", segmentTarFile.getAbsolutePath)

    // push to savePath
    val fs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(savePath), new org.apache.hadoop.conf.Configuration())
    val destPath = new org.apache.hadoop.fs.Path(savePath+ "/" + segmentTarFile.getName)

    fs.copyFromLocalFile(new org.apache.hadoop.fs.Path(segmentTarFile.getAbsolutePath), destPath)
    printf("Copied segment tar file to: %s\n", savePath)

    new SuccessWriterCommitMessage
  }

  override def abort(): Unit = {
    println("Abort called")
  }

  override def close(): Unit = {
    println("Close called")
  }

  private def initSegmentCreator(): Unit = {
    val indexingConfig = new IndexingConfig
    val segmentsValidationAndRetentionConfig = new SegmentsValidationAndRetentionConfig();
    tableConfig =
      new TableConfig(
        tableName,
        "OFFLINE",
        segmentsValidationAndRetentionConfig,
        new TenantConfig(null, null, null),
        indexingConfig,
        new TableCustomConfig(null),
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        false,
        null,
        null,
        null);

    // create a temp dir to store artifacts
    // TODO: savePath could be used directly
    localOutputTempDir = Files.createTempDirectory("pinot-spark-connector").toFile

    segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, pinotSchema)
    segmentGeneratorConfig.setTableName(tableName)
    segmentGeneratorConfig.setSegmentName(segmentNameFormat.format(partitionId))
    segmentGeneratorConfig.setOutDir(localOutputTempDir.getAbsolutePath)
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
        case org.apache.spark.sql.types.BinaryType =>
          gr.putValue(field.name, record.getBinary(idx))
        case _ =>
          throw new UnsupportedOperationException("Unsupported data type")
      }
    }
    gr
  }

  private def tarSegmentDir(segmentName: String): File = {
    val localSegmentDir = new File(localOutputTempDir, segmentName)
    val segmentTarFileName = segmentName + Constants.TAR_GZ_FILE_EXT
    val tarFile = new File(localOutputTempDir, segmentTarFileName)
    printf("Local segment dir: %s\n", localSegmentDir.getAbsolutePath)
    TarGzCompressionUtils.createTarGzFile(localSegmentDir, tarFile)
    val uncompressedSegmentSize = FileUtils.sizeOf(localSegmentDir)
    val compressedSegmentSize = FileUtils.sizeOf(tarFile)
    printf("Size for segment: %s, uncompressed: %s, compressed: %s\n", segmentName, DataSizeUtils.fromBytes(uncompressedSegmentSize), DataSizeUtils.fromBytes(compressedSegmentSize))
    tarFile
  }

}

class SuccessWriterCommitMessage extends WriterCommitMessage {}

