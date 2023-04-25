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

import org.apache.pinot.connector.spark.common.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

/**
 * Example object to test connector with all of features.
 * To run this class, first of all,
 * run pinot locally(https://docs.pinot.apache.org/basics/getting-started/running-pinot-locally)
 */
object ExampleSparkPinotConnectorTest extends Logging {

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession
      .builder()
      .appName("spark-pinot-connector-test")
      .master("local")
      .getOrCreate()

    readOffline()
    readHybrid()
    readHybridWithSpecificSchema()
    readHybridWithFilters()
    readHybridViaGrpc()
    readRealtimeViaGrpc()
    readRealtimeWithFilterViaGrpc()
    readHybridWithFiltersViaGrpc()
    readRealtimeWithSelectionColumns()
    applyJustSomeFilters()
  }

  def readOffline()(implicit spark: SparkSession): Unit = {
    import spark.implicits._
    log.info("## Reading `airlineStats_OFFLINE` table... ##")
    val data = spark.read
      .format("pinot")
      .option("table", "airlineStats")
      .option("tableType", "offline")
      .load()

    data.show()
  }

  def readHybrid()(implicit spark: SparkSession): Unit = {
    log.info("## Reading `airlineStats_OFFLINE and airlineStats_REALTIME` tables... ##")
    val data = spark.read
      .format("pinot")
      .option("table", "airlineStats")
      .option("tableType", "hybrid")
      .load()

    data.show()
  }

  def readHybridWithSpecificSchema()(implicit spark: SparkSession): Unit = {
    log.info("## Reading `airlineStats_OFFLINE and airlineStats_REALTIME` tables with specific schema... ##")
    val schema = StructType(
      Seq(
        StructField("Distance", DataTypes.IntegerType),
        StructField("AirlineID", DataTypes.IntegerType),
        StructField("DaysSinceEpoch", DataTypes.IntegerType),
        StructField("DestStateName", DataTypes.StringType),
        StructField("Origin", DataTypes.StringType),
        StructField("Carrier", DataTypes.StringType)
      )
    )

    val data = spark.read
      .format("pinot")
      .option("table", "airlineStats")
      .option("tableType", "HYBRID")
      .schema(schema)
      .load()

    data.show()
  }

  def readOfflineWithFilters()(implicit spark: SparkSession): Unit = {
    import spark.implicits._
    log.info("## Reading `airlineStats_OFFLINE` table with filter push down... ##")
    val data = spark.read
      .format("pinot")
      .option("table", "airlineStats")
      .option("tableType", "OFFLINE")
      .load()
      .filter($"AirlineID" === 19805)
      .filter($"DestStateName" === "Florida")
      .filter($"DaysSinceEpoch".isin(16101, 16084, 16074))
      .filter($"Origin" === "ORD")

    data.show()
  }

  def readHybridWithFilters()(implicit spark: SparkSession): Unit = {
    import spark.implicits._
    log.info("## Reading `airlineStats_OFFLINE and airlineStats_REALTIME` tables with filter push down... ##")
    // should return 1 data, because connector ensure that the overlap
    // between realtime and offline segment data is queried exactly once
    val data = spark.read
      .format("pinot")
      .option("table", "airlineStats")
      .option("tableType", "hybrid")
      .load()
      .filter($"AirlineID" === 19805)
      .filter($"DestStateName" === "Florida")
      .filter($"DaysSinceEpoch".isin(16101, 16084, 16074))
      .filter($"Origin" === "ORD")

    data.show()
  }

  def readRealtimeWithSelectionColumns()(implicit spark: SparkSession): Unit = {
    import spark.implicits._
    log.info("## Reading `airlineStats_REALTIME` table with column push down... ##")
    val data = spark.read
      .format("pinot")
      .option("table", "airlineStats")
      .option("tableType", "realtime")
      .load()
      .select($"FlightNum", $"Origin", $"DestStateName")

    data.show()
  }

  def readHybridViaGrpc()(implicit spark: SparkSession): Unit = {
    log.info("## Reading `airlineStats` table... ##")
    val data = spark.read
      .format("pinot")
      .option("table", "airlineStats")
      .option("tableType", "hybrid")
      .option("useGrpcServer", "true")
      .load()

    data.show()
  }

  def readRealtimeViaGrpc()(implicit spark: SparkSession): Unit = {
    log.info("## Reading `airlineStats_REALTIME` table... ##")
    val data = spark.read
      .format("pinot")
      .option("table", "airlineStats")
      .option("tableType", "realtime")
      .option("useGrpcServer", "true")
      .load()

    data.show()
  }

  def readRealtimeWithFilterViaGrpc()(implicit spark: SparkSession): Unit = {
    import spark.implicits._
    log.info("## Reading `airlineStats_REALTIME` table... ##")
    val data = spark.read
      .format("pinot")
      .option("table", "airlineStats")
      .option("tableType", "realtime")
      .option("useGrpcServer", "true")
      .load()
      .filter($"DestWac" === 5)
      .select($"FlightNum", $"Origin", $"DestStateName")

    data.show()
  }

  def readHybridWithFiltersViaGrpc()(implicit spark: SparkSession): Unit = {
    import spark.implicits._
    log.info("## Reading `airlineStats_OFFLINE` table with filter push down... ##")
    val data = spark.read
      .format("pinot")
      .option("table", "airlineStats")
      .option("tableType", "hybrid")
      .option("useGrpcServer", "true")
      .load()
      .filter($"DestStateName" === "Florida")

    data.show()
  }


  def applyJustSomeFilters()(implicit spark: SparkSession): Unit = {
    import spark.implicits._
    log.info("## Reading `airlineStats_OFFLINE and airlineStats_REALTIME` tables with filter push down... ##")
    val data = spark.read
      .format("pinot")
      .option("table", "airlineStats")
      .option("tableType", "hybrid")
      .load()
      .filter($"DestStateName" === "Florida")
      .filter($"Origin" === "ORD")
      .select($"DestStateName", $"Origin", $"Distance", $"AirlineID")

    data.show()
  }

}
