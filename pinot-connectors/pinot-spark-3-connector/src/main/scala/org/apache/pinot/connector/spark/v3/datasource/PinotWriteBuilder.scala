package org.apache.pinot.connector.spark.v3.datasource

import org.apache.spark.sql.connector.write.{BatchWrite, LogicalWriteInfo, SupportsOverwrite, Write, WriteBuilder}
import org.apache.spark.sql.sources.Filter

class PinotWriteBuilder(
                         filters: Array[Filter]
                       )
  extends WriteBuilder with SupportsOverwrite {

  override def build(): Write = {
    new PinotWrite()
  }

  override def overwrite(filters: Array[Filter]): WriteBuilder = {
    new PinotWriteBuilder(filters)
  }
}
