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
package org.apache.pinot.connector.spark.common

import java.util

object PinotDataSourceWriteOptions {
  val CONFIG_TABLE_NAME = "table"
  val CONFIG_SEGMENT_NAME_FORMAT = "segmentNameFormat"
  val CONFIG_PATH = "path"
  val CONFIG_INVERTED_INDEX_COLUMNS = "invertedIndexColumns"
  val CONFIG_NO_DICTIONARY_COLUMNS = "noDictionaryColumns"
  val CONFIG_BLOOM_FILTER_COLUMNS = "bloomFilterColumns"
  val CONFIG_RANGE_INDEX_COLUMNS = "rangeIndexColumns"
  val CONFIG_TIME_COLUMN_NAME = "timeColumnName"
  val CONFIG_TIME_FORMAT = "timeFormat"
  val CONFIG_TIME_GRANULARITY = "timeGranularity"

  private[pinot] def from(options: util.Map[String, String]): PinotDataSourceWriteOptions = {
    if (!options.containsKey(CONFIG_TABLE_NAME)) {
      throw new IllegalStateException("Table name must be specified.")
    }
    if (!options.containsKey(CONFIG_PATH)) {
      throw new IllegalStateException("Save path must be specified.")
    }

    val tableName = options.get(CONFIG_TABLE_NAME)
    val savePath = options.get(CONFIG_PATH)
    val segmentNameFormat = options.getOrDefault(CONFIG_SEGMENT_NAME_FORMAT, s"""$tableName-{partitionId:03}""")
    val invertedIndexColumns = options.getOrDefault(CONFIG_INVERTED_INDEX_COLUMNS, "").split(",").filter(_.nonEmpty)
    val noDictionaryColumns = options.getOrDefault(CONFIG_NO_DICTIONARY_COLUMNS, "").split(",").filter(_.nonEmpty)
    val bloomFilterColumns = options.getOrDefault(CONFIG_BLOOM_FILTER_COLUMNS, "").split(",").filter(_.nonEmpty)
    val rangeIndexColumns = options.getOrDefault(CONFIG_RANGE_INDEX_COLUMNS, "").split(",").filter(_.nonEmpty)
    val timeColumnName = options.getOrDefault(CONFIG_TIME_COLUMN_NAME, null)
    val timeFormat = options.getOrDefault(CONFIG_TIME_FORMAT, null)
    val timeGranularity = options.getOrDefault(CONFIG_TIME_GRANULARITY, null)

    if (tableName == null) {
      throw new IllegalArgumentException("Table name is required")
    }
    if (savePath == null) {
      throw new IllegalArgumentException("Save path is required")
    }
    if (segmentNameFormat == "") {
      throw new IllegalArgumentException("Segment name format cannot be empty string")
    }
    if (timeColumnName != null) {
      if (timeFormat.isEmpty == null) {
        throw new IllegalArgumentException("Time format is required when time column name is specified")
      }
      if (timeGranularity == null) {
        throw new IllegalArgumentException("Time granularity is required when time column name is specified")
      }
    }

    PinotDataSourceWriteOptions(
      tableName,
      segmentNameFormat,
      savePath,
      timeColumnName,
      timeFormat,
      timeGranularity,
      invertedIndexColumns,
      noDictionaryColumns,
      bloomFilterColumns,
      rangeIndexColumns
    )
  }
}

// Serializable options for writing data to Pinot
private[pinot] case class PinotDataSourceWriteOptions(
                                                       tableName: String,
                                                       segmentNameFormat: String,
                                                       savePath: String,
                                                       timeColumnName: String,
                                                       timeFormat: String,
                                                       timeGranularity: String,
                                                       invertedIndexColumns: Array[String],
                                                       noDictionaryColumns: Array[String],
                                                       bloomFilterColumns: Array[String],
                                                       rangeIndexColumns: Array[String]
                                                     )
