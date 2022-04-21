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
package org.apache.pinot.controller.api.resources;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import java.io.IOException;
import java.util.List;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.spi.config.table.ColumnStats;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Api(tags = Constants.UPSERT_RESOURCE_TAG)
@Path("/")
public class PinotUpsertCapacityEstimationRestletResource {

  public static final Logger LOGGER = LoggerFactory.getLogger(PinotUpsertCapacityEstimationRestletResource.class);

  /**
   * The API to estimate heap usage for a Pinot upsert table.
   *
   * Sample usage: provide tableConfig, tableSchema, and ColumnStats payload.
   *
   * The tool calculates heap usage by estimating total Key/Value space based on unique key combinations.
   * It used the following formula
   * ```
   * TotalHeapSize = uniqueCombinations * (BytesPerKey + BytesPerValue).
   * ```
   * The following params need to be provided:
   * ```
   * -schemaFile, it contains primary key information.
   * -tableConfigFile, it contains upsertConfig, tablePartitionConfig etc.
   * -columnStats, which stores column information, collected from kafka or staging pinot table.
   * ```
   * For columns stats, we need to gather the following stats
   * ```
   * -cardinality, a required information unique combination of primary keys.
   * -primaryKeySize, it uses for calculating BytesPerKey.
   * -comparisonColSize, it uses for calculating BytesPerValue.
   * -partitionNums(optional), it uses for host assignment calculation.
   * ```
   */
  @POST
  @Path("/heapUsage")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Estimate memory usage for an upsert table", notes =
      "This API returns the estimated heap usage based on primary key column stats."
          + " This allows us to estimate table size before onboarding.")
  public String estimateHeapUsage(@QueryParam("columnStats") String columnStatsStr,
      TableAndSchemaConfig tableSchemaConfig) {
    ObjectNode resultData = JsonUtils.newObjectNode();

    TableConfig tableConfig = tableSchemaConfig.getTableConfig();
    resultData.put("tableName", tableConfig.getTableName());

    Schema schema = tableSchemaConfig.getSchema();
    ColumnStats columnStats;
    try {
      columnStats = JsonUtils.stringToObject(columnStatsStr, ColumnStats.class);
    } catch (IOException e) {
      String msg = String.format("Invalid column stats json string: %s", columnStatsStr);
      throw new ControllerApplicationException(LOGGER, msg, Response.Status.BAD_REQUEST, e);
    }

    // Estimated key space, it contains primary key columns
    int bytesPerKey = 0;
    List<String> primaryKeys = schema.getPrimaryKeyColumns();

    if (columnStats.getPrimaryKeySize() != 0) {
      bytesPerKey += columnStats.getPrimaryKeySize();
    } else {
      for (String primaryKey : primaryKeys) {
        FieldSpec.DataType dt = schema.getFieldSpecFor(primaryKey).getDataType();
        if (dt == FieldSpec.DataType.JSON || dt == FieldSpec.DataType.LIST || dt == FieldSpec.DataType.MAP) {
          String msg = "Not support data types for primary key columns";
          throw new ControllerApplicationException(LOGGER, msg, Response.Status.BAD_REQUEST);
        } else if (dt == FieldSpec.DataType.STRING) {
          String msg = "Missing primary key sizes for String columns";
          throw new ControllerApplicationException(LOGGER, msg, Response.Status.BAD_REQUEST);
        } else {
          bytesPerKey += dt.size();
        }
      }
      // Java has a 24 bytes array overhead and there's also 8 bytes for the actual array object
      bytesPerKey += 32;
    }

    // Estimated value space, it contains <segmentName, DocId, ComparisonValue(timestamp)>
    int bytesPerValue =
        tableConfig.getUpsertConfig().getComparisonColumn() != null ? 52 + columnStats.getComparisonColSize() : 64;
    resultData.put("bytesPerKey", bytesPerKey);
    resultData.put("bytesPerValue", bytesPerValue);

    long primaryKeyCardinality = columnStats.getCardinality();
    long totalKeySpace = bytesPerKey * primaryKeyCardinality;
    long totalValueSpace = bytesPerValue * primaryKeyCardinality;
    long totalSpace = totalKeySpace + totalValueSpace;

    resultData.put("totalKeySpace(bytes)", totalKeySpace);
    resultData.put("totalValueSpace(bytes)", totalValueSpace);
    resultData.put("totalSpace(bytes)", totalSpace);

    // Use Partitions, replicas and host assignment.
    if (columnStats.getPartitionsNum() > 0) {
      int partitionsNum = columnStats.getPartitionsNum();
      int replicasPerPartition = tableConfig.getValidationConfig().getReplicasPerPartitionNumber();
      double memoryPerHost = (totalSpace * replicasPerPartition * 1.0) / partitionsNum;

      resultData.put("numPartitions", partitionsNum);
      resultData.put("replicasPerPartition", replicasPerPartition);
      resultData.put("memoryPerHost", memoryPerHost);
    }
    return resultData.toString();
  }
}
