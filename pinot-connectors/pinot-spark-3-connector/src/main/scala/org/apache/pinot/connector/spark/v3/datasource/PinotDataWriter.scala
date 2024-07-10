package org.apache.pinot.connector.spark.v3.datasource

import org.apache.spark.sql.connector.write.{DataWriter, PhysicalWriteInfo, WriterCommitMessage}
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig
import org.apache.pinot.spi.config.table.TableConfig
import org.apache.pinot.spi.data.readers.RecordReader

class PinotDataWriter[InternalRow](physicalWriteInfo: PhysicalWriteInfo,
                                    partitionId: Int,
                                    taskId: Long)
  extends DataWriter[InternalRow] with AutoCloseable {

  private val tableConfig = new TableConfig()

  // initialize
  private val segmentGeneratorConfig = new SegmentGeneratorConfig(new TableConfig(), new RecordReader()
  segmentGeneratorConfig.setTableName("testTable")
  segmentGeneratorConfig.setSegmentName("segmentName")
  segmentGeneratorConfig.setOutDir("tempDir")
  segmentGeneratorConfig.setRecordReaderPath("recordReaderPath")

  override def write(record: InternalRow): Unit = {



    val driver = new SegmentIndexCreationDriverImpl()
    driver.init(S
    driver.build()

  }

  override def commit(): WriterCommitMessage = {



    null
  }

  override def abort(): Unit = {
    null
  }

  override def close(): Unit = {
    null
  }
}