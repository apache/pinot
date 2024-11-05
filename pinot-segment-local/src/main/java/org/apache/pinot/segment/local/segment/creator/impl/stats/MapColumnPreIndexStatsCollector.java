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
package org.apache.pinot.segment.local.segment.creator.impl.stats;

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import java.util.Arrays;
import java.util.Map;
import org.apache.pinot.common.utils.PinotDataType;
import org.apache.pinot.segment.spi.creator.StatsCollectorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.MapUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;


/**
 * Extension of {@link AbstractColumnStatisticsCollector} for Map column type.
 *
 * The Map column type is different than other columns in that it is essentially recursive.  It contains keys
 * and those keys are analogous to columns and, as such, have Key level statistics. So, this class keeps track of
 * Map column level statistics _and_ Key level statistics.  The Key Level statistics can then be used during
 * the creation of the Immutable Segment to make decisions about how keys will be stored or what Map data structure
 * to use.
 *
 * Assumptions that are made:
 * 1. Each key has a single type for the value's associated with it across all documents.
 * 2. At this point in the  Pinot process, the type consistency of a key should already be enforced, so if a
 * heterogeneous value types for a key are encountered will construct the Map statistics it can be raised as a fault.
 */
public class MapColumnPreIndexStatsCollector extends AbstractColumnStatisticsCollector {
  private final Object2ObjectOpenHashMap<String, AbstractColumnStatisticsCollector> _keyStats =
      new Object2ObjectOpenHashMap<>(INITIAL_HASH_SET_SIZE);
  private String[] _sortedValues;
  private int _minLength = Integer.MAX_VALUE;
  private int _maxLength = 0;
  private boolean _sealed = false;

  public MapColumnPreIndexStatsCollector(String column, StatsCollectorConfig statsCollectorConfig) {
    super(column, statsCollectorConfig);
    _sorted = false;
  }

  public AbstractColumnStatisticsCollector getKeyStatistics(String key) {
    return _keyStats.get(key);
  }

  @Override
  public void collect(Object entry) {
    assert !_sealed;

    if (entry instanceof Map) {
      //noinspection unchecked
      Map<String, Object> mapValue = (Map<String, Object>) entry;
      int length = MapUtils.serializeMap(mapValue).length;
      _minLength = Math.min(_minLength, length);
      _maxLength = Math.max(_maxLength, length);

      for (Map.Entry<String, Object> mapValueEntry : mapValue.entrySet()) {
        String key = mapValueEntry.getKey();
        Object value = mapValueEntry.getValue();
        AbstractColumnStatisticsCollector keyStats = _keyStats.get(key);
        if (keyStats == null) {
          keyStats = createKeyStatsCollector(key, value);
          _keyStats.put(key, keyStats);
          if (isPartitionEnabled()) {
            updatePartition(key);
          }
        }
        keyStats.collect(value);
      }
      _totalNumberOfEntries++;
    } else {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public String getMinValue() {
    if (_sealed) {
      return _sortedValues[0];
    }
    throw new IllegalStateException("you must seal the collector first before asking for min value");
  }

  @Override
  public String getMaxValue() {
    if (_sealed) {
      return _sortedValues[_sortedValues.length - 1];
    }
    throw new IllegalStateException("you must seal the collector first before asking for max value");
  }

  @Override
  public String[] getUniqueValuesSet() {
    if (_sealed) {
      return _sortedValues;
    }
    throw new IllegalStateException("you must seal the collector first before asking for unique values set");
  }

  @Override
  public int getLengthOfShortestElement() {
    return _minLength;
  }

  @Override
  public int getLengthOfLargestElement() {
    return _maxLength;
  }

  @Override
  public int getMaxRowLengthInBytes() {
    return _maxLength;
  }

  @Override
  public int getCardinality() {
    return _keyStats.size();
  }

  @Override
  public void seal() {
    if (!_sealed) {
      _sortedValues = _keyStats.keySet().toArray(new String[0]);
      Arrays.sort(_sortedValues);

      // Iterate through every key stats collector and seal them
      for (AbstractColumnStatisticsCollector keyStatsCollector : _keyStats.values()) {
        keyStatsCollector.seal();
      }

      _sealed = true;
    }
  }

  /**
   * Create a Stats Collector for each Key in the Map.
   *
   * NOTE: this could raise an issue if there are millions of keys with very few values (Sparse keys, in other words).
   * So a less memory intensive option may be better for this.
   */
  private AbstractColumnStatisticsCollector createKeyStatsCollector(String key, Object value) {
    // Get the type of the value
    PinotDataType type = PinotDataType.getSingleValueType(value.getClass());
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(key).build();
    Schema keySchema = new Schema.SchemaBuilder().setSchemaName(key)
        .addField(new DimensionFieldSpec(key, convertToDataType(type), false)).build();
    StatsCollectorConfig config = new StatsCollectorConfig(tableConfig, keySchema, null);

    switch (type) {
      case INTEGER:
        return new IntColumnPreIndexStatsCollector(key, config);
      case LONG:
        return new LongColumnPreIndexStatsCollector(key, config);
      case FLOAT:
        return new FloatColumnPreIndexStatsCollector(key, config);
      case DOUBLE:
        return new DoubleColumnPreIndexStatsCollector(key, config);
      case BIG_DECIMAL:
        return new BigDecimalColumnPreIndexStatsCollector(key, config);
      case STRING:
        return new StringColumnPreIndexStatsCollector(key, config);
      default:
        throw new UnsupportedOperationException(String.format("MAP column does not yet support '%s'", type));
    }
  }

  static FieldSpec.DataType convertToDataType(PinotDataType ty) {
    // TODO: I've been told that we already have a function to do this, so find that function and replace this
    switch (ty) {
      case BOOLEAN:
        return FieldSpec.DataType.BOOLEAN;
      case SHORT:
      case INTEGER:
        return FieldSpec.DataType.INT;
      case LONG:
        return FieldSpec.DataType.LONG;
      case FLOAT:
        return FieldSpec.DataType.FLOAT;
      case DOUBLE:
        return FieldSpec.DataType.DOUBLE;
      case BIG_DECIMAL:
        return FieldSpec.DataType.BIG_DECIMAL;
      case TIMESTAMP:
        return FieldSpec.DataType.TIMESTAMP;
      case TIMESTAMP_NTZ:
        return FieldSpec.DataType.TIMESTAMP_NTZ;
      case DATE:
        return FieldSpec.DataType.DATE;
      case TIME:
        return FieldSpec.DataType.TIME;
      case STRING:
        return FieldSpec.DataType.STRING;
      default:
        throw new UnsupportedOperationException();
    }
  }
}
