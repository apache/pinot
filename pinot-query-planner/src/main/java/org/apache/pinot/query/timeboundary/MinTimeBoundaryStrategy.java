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

import com.google.common.base.Preconditions;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.core.routing.TimeBoundaryInfo;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


public class MinTimeBoundaryStrategy implements TimeBoundaryStrategy {
  private final Set<String> _includedTables;
  private final Set<String> _excludedTables;

  public MinTimeBoundaryStrategy(Map<String, Object> parameters) {
    if (parameters.containsKey("includedTables")) {
      Preconditions.checkArgument(parameters.get("includedTables") instanceof List,
          "Included tables should be a list");
      _includedTables = new HashSet<>((List<String>) parameters.get("includedTables"));
    } else {
      _includedTables = new HashSet<>();
    }

    if (parameters.containsKey("excludedTables")) {
      Preconditions.checkArgument(parameters.get("excludedTables") instanceof List,
          "Excluded tables should be a list");
      _excludedTables = new HashSet<>((List<String>) parameters.get("excludedTables"));
    } else {
      _excludedTables = new HashSet<>();
    }
  }

  @Override
  public TimeBoundaryInfo computeTimeBoundary(TableCache tableCache, List<String> physicalTableNames,
      RoutingManager routingManager) {
    TimeBoundaryInfo minTimeBoundaryInfo = null;
    long minTimeBoundary = Long.MAX_VALUE;
    for (String physicalTableName : physicalTableNames) {
      TimeBoundaryInfo current = routingManager.getTimeBoundaryInfo(physicalTableName);
      if (current != null && (_includedTables.isEmpty() || _includedTables.contains(physicalTableName))
          && !_excludedTables.contains(physicalTableName)) {
        String rawTableName = TableNameBuilder.extractRawTableName(physicalTableName);
        Schema schema = tableCache.getSchema(rawTableName);
        TableConfig tableConfig = tableCache.getTableConfig(physicalTableName);
        Preconditions.checkArgument(tableConfig != null,
            "Table config not found for table: %s", physicalTableName);
        Preconditions.checkArgument(schema != null,
            "Schema not found for table: %s", physicalTableName);
        String timeColumnName = tableConfig.getValidationConfig().getTimeColumnName();
        DateTimeFieldSpec dateTimeFieldSpec = schema.getSpecForTimeColumn(timeColumnName);
        Preconditions.checkArgument(dateTimeFieldSpec != null,
            "Time column not found in schema for table: %s", physicalTableName);
        DateTimeFormatSpec specFormatSpec = dateTimeFieldSpec.getFormatSpec();
        long currentTimeBoundaryMillis = specFormatSpec.fromFormatToMillis(current.getTimeValue());
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
}
