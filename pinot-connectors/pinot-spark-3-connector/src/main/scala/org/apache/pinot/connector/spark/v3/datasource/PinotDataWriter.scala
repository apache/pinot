package org.apache.pinot.connector.spark.v3.datasource

import org.apache.spark.sql.connector.write.{DataWriter, PhysicalWriteInfo, WriterCommitMessage}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.pinot.segment.local.segment.creator.impl.SegmentColumnarIndexCreator
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig
import org.apache.pinot.spi.config.table.{IndexingConfig, SegmentsValidationAndRetentionConfig, TableConfig, TableCustomConfig, TenantConfig}
import org.apache.pinot.spi.data.readers.GenericRow
import org.apache.pinot.spi.data.{FieldSpec, Schema}
import org.apache.pinot.spi.data.Schema.SchemaBuilder

import java.io.File
import java.nio.file.Files;

class PinotDataWriter[InternalRow](physicalWriteInfo: PhysicalWriteInfo,
                                    partitionId: Int,
                                    taskId: Long)
  extends DataWriter[org.apache.spark.sql.catalyst.InternalRow] with AutoCloseable {

  println("PinotDataWriter created with writeInfo: " + physicalWriteInfo + ", partitionId: " + partitionId + ", taskId: " + taskId)

  val indexingConfig = new IndexingConfig
  val segmentsValidationAndRetentionConfig = new SegmentsValidationAndRetentionConfig();
  private val tableConfig =
    new TableConfig(
      "myTable",
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

  private val schema = new SchemaBuilder()
    .addSingleValueDimension("column1", FieldSpec.DataType.STRING)
    .addSingleValueDimension("column2", FieldSpec.DataType.STRING)
    .addMetric("column3", FieldSpec.DataType.INT)
    .build()

  // initialize
  private val segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema)
  segmentGeneratorConfig.setTableName("testTable")
  segmentGeneratorConfig.setSegmentName("segmentName")
  segmentGeneratorConfig.setOutDir("tempDir" + partitionId.toString + taskId.toString)
  segmentGeneratorConfig.setRecordReaderPath("recordReaderPath")

  // pick a random integer
  val randint = scala.util.Random.nextInt(1000000)
  val tempDir = Files.createTempDirectory("pinotSegmentTempDir" + randint.toString).toFile
  println("Temp dir: " + tempDir.getAbsolutePath)

  // Check if has star tree
  private val indexCreator = new SegmentColumnarIndexCreator();
  indexCreator.setSegmentName("mySegment")
  indexCreator.init(
    segmentGeneratorConfig,
    null,
    null,
    schema,
    tempDir,
    null)

  override def write(record: org.apache.spark.sql.catalyst.InternalRow): Unit = {
    indexCreator.indexRow(internalRowToGenericRow(record))
  }

  def internalRowToGenericRow(record: org.apache.spark.sql.catalyst.InternalRow): GenericRow = {
    val gr = new GenericRow()

    gr.putValue("column1", record.getString(0))
    gr.putValue("column2", record.getString(1))
    gr.putValue("column3", record.getInt(2))

    gr
  }

  override def commit(): WriterCommitMessage = {
    indexCreator.seal()
    indexCreator.close()

    new SuccessWriterCommitMessage
  }

  override def abort(): Unit = {}

  override def close(): Unit = {}
}

class SuccessWriterCommitMessage extends WriterCommitMessage {
  def writerCommitMessage: String = "Commit successful"
}