package org.apache.pinot.connector.spark.v3.datasource

import org.apache.pinot.spi.data.{FieldSpec, Schema}
import org.apache.pinot.spi.data.Schema.SchemaBuilder
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriterFactory, LogicalWriteInfo, PhysicalWriteInfo, Write, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

class PinotWrite (
                 logicalWriteInfo: LogicalWriteInfo
                 ) extends Write with BatchWrite {

  val _logicalWriteInfo: LogicalWriteInfo = logicalWriteInfo
  val tableName: String = logicalWriteInfo.options().get("table")
  val writeSchema: StructType = logicalWriteInfo.schema()
  val pinotSchema: Schema = SparkToPinotSchemaTranslator.translate(writeSchema)

  var segmentFormat: String = logicalWriteInfo.options().get("segmentFormat")
  val savePath: String = logicalWriteInfo.options().get("path")

  if (tableName == null) {
    throw new IllegalArgumentException("Table name is required")
  }

  if (savePath == null) {
    throw new IllegalArgumentException("Save path is required")
  }

  if (segmentFormat == null) {
    segmentFormat = s"""$tableName-%d"""
  }

  override def createBatchWriterFactory(physicalWriteInfo: PhysicalWriteInfo): DataWriterFactory = {
        val _savePath = savePath
        val _tableName = tableName
        val _segmentFormat = segmentFormat
        val _writeSchema = writeSchema
        val _pinotSchema = pinotSchema
    (partitionId: Int, taskId: Long) => {
      new PinotDataWriter(
        physicalWriteInfo,
        partitionId,
        taskId,
        _tableName,
        _segmentFormat,
        _savePath,
        _writeSchema,
        _pinotSchema,
      )
    }
  }

  override def toBatch: BatchWrite = this

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    messages.foreach(println)
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    messages.foreach(println)
  }

}