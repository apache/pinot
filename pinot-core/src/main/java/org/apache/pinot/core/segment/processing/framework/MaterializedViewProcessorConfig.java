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
package org.apache.pinot.core.segment.processing.framework;

import java.util.Map;
import java.util.Set;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.ingestion.FilterConfig;
import org.apache.pinot.spi.data.Schema;


public class MaterializedViewProcessorConfig {
  private final TableConfig _mvTableConfig;
  private final Schema _mvSchema;
  private final Set<String> _selectedDimensions;
  private final FilterConfig _filterConfig;
  private final Map<String, Set<AggregationFunctionType>> _aggregationFunctionSetMap;

  public TableConfig getMvTableConfig() {
    return _mvTableConfig;
  }

  public Schema getMvSchema() {
    return _mvSchema;
  }

  public Set<String> getSelectedDimensions() {
    return _selectedDimensions;
  }

  public FilterConfig getFilterConfig() {
    return _filterConfig;
  }

  public Map<String, Set<AggregationFunctionType>> getAggregationFunctionSetMap() {
    return _aggregationFunctionSetMap;
  }

  private MaterializedViewProcessorConfig(TableConfig mvTableConfig,
          Schema mvSchema, Set<String> selectedDimensions, FilterConfig filterConfig, Map<String,
          Set<AggregationFunctionType>> aggregationFunctionSetMap) {
    _mvTableConfig = mvTableConfig;
    _mvSchema = mvSchema;
    _selectedDimensions = selectedDimensions;
    _filterConfig = filterConfig;
    _aggregationFunctionSetMap = aggregationFunctionSetMap;
  }

  public static class Builder {
    private TableConfig _mvTableConfig;
    private Schema _mvSchema;
    private Set<String> _selectedDimensions;
    private FilterConfig _filterConfig;
    private Map<String, Set<AggregationFunctionType>> _aggregationFunctionSetMap;

    public Builder setMvTableConfig(TableConfig tableConfig) {
      _mvTableConfig = tableConfig;
      return this;
    }

    public Builder setMvSchema(Schema schema) {
      _mvSchema = schema;
      return this;
    }

    public Builder setSelectedDimensions(Set<String> selectedDimensions) {
      _selectedDimensions = selectedDimensions;
      return this;
    }

    public Builder setFilterConfig(FilterConfig filterConfig) {
      _filterConfig = filterConfig;
      return this;
    }

    public Builder setAggregationFunctionSetMap(Map<String, Set<AggregationFunctionType>> aggregationFunctionSetMap) {
      _aggregationFunctionSetMap = aggregationFunctionSetMap;
      return this;
    }

    public MaterializedViewProcessorConfig build() {
      return new MaterializedViewProcessorConfig(_mvTableConfig, _mvSchema, _selectedDimensions,
          _filterConfig, _aggregationFunctionSetMap);
    }
  }
}
