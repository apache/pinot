package org.apache.pinot.connector.spark.v3.datasource

import org.apache.spark.sql.connector.write.{BatchWrite, DataWriterFactory, PhysicalWriteInfo, Write, WriterCommitMessage}

class PinotWrite extends Write with BatchWrite {
  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
    (partitionId: Int, taskId: Long) => {
      new PinotDataWriter(info, partitionId, taskId)
    }
  }

  override def toBatch: BatchWrite = this

  override def commit(messages: Array[WriterCommitMessage]) = {
    messages.foreach(println)
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    messages.foreach(println)
  }

}