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
import io.swagger.annotations.ApiKeyAuthDefinition;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import java.io.IOException;
import java.util.List;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.Authorize;
import org.apache.pinot.core.auth.TargetType;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


@Api(tags = Constants.UPSERT_RESOURCE_TAG, authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = @ApiKeyAuthDefinition(name =
    HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = SWAGGER_AUTHORIZATION_KEY)))
@Path("/")
public class PinotUpsertRestletResource {

  public static final Logger LOGGER = LoggerFactory.getLogger(PinotUpsertRestletResource.class);

  @Inject
  PinotHelixResourceManager _pinotHelixResourceManager;

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
  @Path("/upsert/estimateHeapUsage")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.ESTIMATE_UPSERT_MEMORY)
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Estimate memory usage for an upsert table", notes =
      "This API returns the estimated heap usage based on primary key column stats."
          + " This allows us to estimate table size before onboarding.")
  public String estimateHeapUsage(String tableSchemaConfigStr,
      @ApiParam(value = "cardinality", required = true) @QueryParam("cardinality") long cardinality,
      @ApiParam(value = "primaryKeySize", defaultValue = "-1") @QueryParam("primaryKeySize") int primaryKeySize,
      @ApiParam(value = "numPartitions", defaultValue = "-1") @QueryParam("numPartitions") int numPartitions,
      @Context HttpHeaders headers) {
    ObjectNode resultData = JsonUtils.newObjectNode();
    TableAndSchemaConfig tableSchemaConfig;

    try {
      tableSchemaConfig = JsonUtils.stringToObject(tableSchemaConfigStr, TableAndSchemaConfig.class);
    } catch (IOException e) {
      throw new ControllerApplicationException(LOGGER,
          String.format("Invalid TableSchemaConfigs json string: %s", tableSchemaConfigStr),
          Response.Status.BAD_REQUEST, e);
    }

    TableConfig tableConfig = tableSchemaConfig.getTableConfig();
    String tableName = _pinotHelixResourceManager.translateTableName(tableConfig.getTableName(),
        headers.getHeaderString(CommonConstants.DATABASE));
    tableConfig.setTableName(tableName);
    Schema schema = tableSchemaConfig.getSchema();

    resultData.put("tableName", tableName);

    // Estimated key space, it contains primary key columns.
    int bytesPerKey = 0;
    List<String> primaryKeys = schema.getPrimaryKeyColumns();

    if (primaryKeySize > 0) {
      bytesPerKey = primaryKeySize;
    } else {
      for (String primaryKey : primaryKeys) {
        FieldSpec.DataType dt = schema.getFieldSpecFor(primaryKey).getDataType();
        if (!dt.isFixedWidth()) {
          String msg = "Primary key sizes much be provided for non fixed-width columns";
          throw new ControllerApplicationException(LOGGER, msg, Response.Status.BAD_REQUEST);
        } else {
          bytesPerKey += dt.size();
        }
      }
      // Java has a 24 bytes array overhead and there's also 8 bytes for the actual array object
      bytesPerKey += 32;
    }

    // Estimated value space, it contains <segmentName, DocId, ComparisonValue(timestamp)> and overhead.
    // Here we only calculate the map content size. TODO: Add the map entry size and the array size within the map.
    int bytesPerValue = 60;
    List<String> comparisonColumns = tableConfig.getUpsertConfig().getComparisonColumns();
    if (comparisonColumns != null) {
      int bytesPerArrayElem = 8;  // object ref
      bytesPerValue = 52;
      for (String columnName : comparisonColumns) {
        FieldSpec.DataType dt = schema.getFieldSpecFor(columnName).getDataType();
        if (!dt.isFixedWidth()) {
          String msg = "Not support data types for the comparison column";
          throw new ControllerApplicationException(LOGGER, msg, Response.Status.BAD_REQUEST);
        } else {
          if (comparisonColumns.size() == 1) {
            bytesPerValue += dt.size();
          } else {
            bytesPerValue += bytesPerArrayElem + dt.size();
          }
        }
      }
      if (comparisonColumns.size() > 1) {
        bytesPerValue += 48 + 4;  // array overhead + comparableIndex integer
      }
    }

    resultData.put("bytesPerKey", bytesPerKey);
    resultData.put("bytesPerValue", bytesPerValue);

    long totalKeySpace = bytesPerKey * cardinality;
    long totalValueSpace = bytesPerValue * cardinality;
    long totalSpace = totalKeySpace + totalValueSpace;

    resultData.put("totalKeySpace(bytes)", totalKeySpace);
    resultData.put("totalValueSpace(bytes)", totalValueSpace);
    resultData.put("totalSpace(bytes)", totalSpace);

    // Use Partitions, replicas to calculate memoryPerHost for host assignment.
    if (numPartitions > 0) {
      double totalSpacePerPartition = (totalSpace * 1.0) / numPartitions;
      resultData.put("numPartitions", numPartitions);
      resultData.put("totalSpacePerPartition(bytes)", totalSpacePerPartition);
    }
    return resultData.toString();
  }
}
