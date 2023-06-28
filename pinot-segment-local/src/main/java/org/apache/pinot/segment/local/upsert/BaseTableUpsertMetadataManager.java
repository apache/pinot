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
package org.apache.pinot.segment.local.upsert;

import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.collections.CollectionUtils;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.spi.config.table.HashFunction;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.Schema;


@ThreadSafe
public abstract class BaseTableUpsertMetadataManager implements TableUpsertMetadataManager {
  protected String _tableNameWithType;
  protected List<String> _primaryKeyColumns;
  protected List<String> _comparisonColumns;
  protected String _deleteRecordColumn;
  protected HashFunction _hashFunction;
  protected PartialUpsertHandler _partialUpsertHandler;
  protected boolean _enableSnapshot;
  protected ServerMetrics _serverMetrics;

  @Override
  public void init(TableConfig tableConfig, Schema schema, TableDataManager tableDataManager,
      ServerMetrics serverMetrics) {
    _tableNameWithType = tableConfig.getTableName();

    UpsertConfig upsertConfig = tableConfig.getUpsertConfig();
    Preconditions.checkArgument(upsertConfig != null && upsertConfig.getMode() != UpsertConfig.Mode.NONE,
        "Upsert must be enabled for table: %s", _tableNameWithType);

    _primaryKeyColumns = schema.getPrimaryKeyColumns();
    Preconditions.checkArgument(!CollectionUtils.isEmpty(_primaryKeyColumns),
        "Primary key columns must be configured for upsert enabled table: %s", _tableNameWithType);

    _comparisonColumns = upsertConfig.getComparisonColumns();
    if (_comparisonColumns == null) {
      _comparisonColumns = Collections.singletonList(tableConfig.getValidationConfig().getTimeColumnName());
    }

    _deleteRecordColumn = upsertConfig.getDeleteRecordColumn();
    _hashFunction = upsertConfig.getHashFunction();

    if (upsertConfig.getMode() == UpsertConfig.Mode.PARTIAL) {
      Map<String, UpsertConfig.Strategy> partialUpsertStrategies = upsertConfig.getPartialUpsertStrategies();
      Preconditions.checkArgument(partialUpsertStrategies != null,
          "Partial-upsert strategies must be configured for partial-upsert enabled table: %s", _tableNameWithType);
      _partialUpsertHandler =
          new PartialUpsertHandler(schema, partialUpsertStrategies, upsertConfig.getDefaultPartialUpsertStrategy(),
              _comparisonColumns);
    }

    _enableSnapshot = upsertConfig.isEnableSnapshot();
    _serverMetrics = serverMetrics;
  }

  @Override
  public UpsertConfig.Mode getUpsertMode() {
    return _partialUpsertHandler == null ? UpsertConfig.Mode.FULL : UpsertConfig.Mode.PARTIAL;
  }
}
