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
package org.apache.pinot.query.timeboundary;

import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.core.routing.TimeBoundaryInfo;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.LogicalTableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


@AutoService(TimeBoundaryStrategy.class)
public class MinTimeBoundaryStrategy implements TimeBoundaryStrategy {

  private static final String INCLUDED_TABLES = "includedTables";
  Map<String, DateTimeFormatSpec> _dateTimeFormatSpecMap;

  @Override
  public void init(LogicalTableConfig logicalTableConfig, TableCache tableCache) {
    List<String> includedTables = getTimeBoundaryTableNames(logicalTableConfig);
    _dateTimeFormatSpecMap = new HashMap<>(includedTables.size());
    for (String physicalTableName : includedTables) {
      String rawTableName = TableNameBuilder.extractRawTableName(physicalTableName);
      Schema schema = tableCache.getSchema(rawTableName);
      TableConfig tableConfig = tableCache.getTableConfig(physicalTableName);
      Preconditions.checkArgument(tableConfig != null, "Table config not found for table: %s", physicalTableName);
      Preconditions.checkArgument(schema != null, "Schema not found for table: %s", physicalTableName);
      String timeColumnName = tableConfig.getValidationConfig().getTimeColumnName();
      DateTimeFieldSpec dateTimeFieldSpec = schema.getSpecForTimeColumn(timeColumnName);
      Preconditions.checkArgument(dateTimeFieldSpec != null, "Time column not found in schema for table: %s",
          physicalTableName);
      DateTimeFormatSpec specFormatSpec = dateTimeFieldSpec.getFormatSpec();
      _dateTimeFormatSpecMap.put(physicalTableName, specFormatSpec);
    }
  }

  @Override
  public String getName() {
    return "min";
  }

  @Override
  public TimeBoundaryInfo computeTimeBoundary(RoutingManager routingManager) {
    TimeBoundaryInfo minTimeBoundaryInfo = null;
    long minTimeBoundary = Long.MAX_VALUE;
    for (Map.Entry<String, DateTimeFormatSpec> entry : _dateTimeFormatSpecMap.entrySet()) {
      TimeBoundaryInfo current = routingManager.getTimeBoundaryInfo(entry.getKey());
      if (current != null) {
        long currentTimeBoundaryMillis = entry.getValue().fromFormatToMillis(current.getTimeValue());
        if (minTimeBoundaryInfo == null) {
          minTimeBoundaryInfo = current;
          minTimeBoundary = currentTimeBoundaryMillis;
        } else if (minTimeBoundary > currentTimeBoundaryMillis) {
          minTimeBoundaryInfo = current;
          minTimeBoundary = currentTimeBoundaryMillis;
        }
      }
    }
    return minTimeBoundaryInfo;
  }

  @Override
  public List<String> getTimeBoundaryTableNames(LogicalTableConfig logicalTableConfig) {
    Map<String, Object> parameters = logicalTableConfig.getTimeBoundaryConfig().getParameters();
    return parameters != null ? (List) parameters.getOrDefault(INCLUDED_TABLES, List.of()) : List.of();
  }
}
