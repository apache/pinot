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
    val spark: SparkSession = SparkSession
      .builder()
      .appName("spark-pinot-connector-test")
      //.master("local")
      .getOrCreate()
    import spark.implicits._  
    val data = spark.read
  .format("pinot")
  .option("table", "workflowEvents")
  .option("tableType", "REALTIME")
  .option("controller", "a7fb11413d1654898aa0a1b40d3c0921-7193d974e9e64501.elb.us-east-1.amazonaws.com:80")
  .option("broker", "a9ffe252f10244776b63ae7516b6ea24-cbd1083916d6d938.elb.us-east-1.amazonaws.com:80")
  .option("useGrpcServer", "true")
  .load()
  //.filter($"DestStateName" === "Florida")
  data.show(10)
  //println(data.count())
  data.write.format("json").mode("Overwrite")
  .save("file:///var/pinot/minion/data/temp")

  }

}
