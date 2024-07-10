package org.apache.pinot.connector.spark.v3.datasource

import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.connector.write.SupportsOverwrite
import org.apache.spark.sql.sources.Filter

class PinotWriteBuilder extends WriteBuilder
  with SupportsOverwrite {

  override def build(): PinotWrite = {
    new PinotWrite()
  }

  override def overwrite(filters: Array[Filter]): WriteBuilder = {

    new PinotWrite()
  }
}
