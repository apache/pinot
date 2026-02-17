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

import com.fasterxml.jackson.core.JsonProcessingException;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Map;
import org.apache.pinot.common.utils.PinotDataType;
import org.apache.pinot.segment.spi.creator.StatsCollectorConfig;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.FieldIndexConfigsUtil;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.MapUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Extension of {@link AbstractColumnStatisticsCollector} for Map column type.
 *
 * The Map column type is different from other columns in that it is essentially recursive. It contains keys
 * and those keys are analogous to columns and, as such, have Key level statistics. So, this class keeps track of
 * Map column level statistics _and_ Key level statistics. The Key Level statistics can then be used during
 * the creation of the Immutable Segment to make decisions about how keys will be stored or what Map data structure
 * to use.
 *
 * Assumptions that are made:
 * 1. Each key has a single type for the value's associated with it across all documents.
 * 2. At this point in the  Pinot process, the type consistency of a key should already be enforced, so if a
 * heterogeneous value types for a key are encountered will construct the Map statistics it can be raised as a fault.
 */
public class MapColumnPreIndexStatsCollector extends AbstractColumnStatisticsCollector {
  private static final Logger LOGGER = LoggerFactory.getLogger(MapColumnPreIndexStatsCollector.class);
  private final Object2ObjectOpenHashMap<String, AbstractColumnStatisticsCollector> _keyStats =
      new Object2ObjectOpenHashMap<>(INITIAL_HASH_SET_SIZE);
  private final Map<String, Integer> _keyFrequencies = new Object2ObjectOpenHashMap<>(INITIAL_HASH_SET_SIZE);
  private final Object2ObjectOpenHashMap<String, Object> _firstRawValuesByKey =
      new Object2ObjectOpenHashMap<>(INITIAL_HASH_SET_SIZE);
  private String[] _sortedKeys;
  private int _minLength = Integer.MAX_VALUE;
  private int _maxLength = 0;
  private boolean _sealed = false;
  private boolean _createNoDictCollectorsForKeys = false;

  public MapColumnPreIndexStatsCollector(String column, StatsCollectorConfig statsCollectorConfig) {
    super(column, statsCollectorConfig);
    _sorted = false;
    Map<String, FieldIndexConfigs> indexConfigsByCol = FieldIndexConfigsUtil.createIndexConfigsByColName(
        statsCollectorConfig.getTableConfig(), statsCollectorConfig.getSchema());
    boolean isDictionaryEnabled = indexConfigsByCol.get(column).getConfig(StandardIndexes.dictionary()).isEnabled();
    if (!isDictionaryEnabled) {
      _createNoDictCollectorsForKeys = statsCollectorConfig.getTableConfig().getIndexingConfig()
          .isOptimizeNoDictStatsCollection();
    }
  }

  public AbstractColumnStatisticsCollector getKeyStatistics(String key) {
    return _keyStats.get(key);
  }

  public Map<String, Integer> getAllKeyFrequencies() {
    return _keyFrequencies;
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
        if (value == null) {
          continue;
        }
        _keyFrequencies.merge(key, 1, Integer::sum);
        AbstractColumnStatisticsCollector keyStats = _keyStats.get(key);
        if (keyStats == null) {
          keyStats = createKeyStatsCollector(key, value);
          _keyStats.put(key, keyStats);
          _firstRawValuesByKey.put(key, value);
          if (isPartitionEnabled()) {
            updatePartition(key);
          }
        }
        if (keyStats instanceof NoDictColumnStatisticsCollector) {
          keyStats.collect(value);
          continue;
        }
        if (keyStats instanceof StringColumnPreIndexStatsCollector) {
          if (value instanceof String || value instanceof Number || value instanceof Boolean) {
            keyStats.collect(String.valueOf(value));
            continue;
          }
          try {
            keyStats.collect(JsonUtils.objectToString(value));
            continue;
          } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize value for key '" + key + "': " + value, e);
          }
        }
        if (keyStats instanceof BigDecimalColumnPreIndexStatsCollector) {
          try {
            keyStats.collect(new BigDecimal(value.toString()));
          } catch (NumberFormatException e) {
            LOGGER.warn("Could not parse map key '{}' value '{}' as BIG_DECIMAL; promoting to STRING collector",
                key, value);
            keyStats = promoteNumericKeyStatsToStringCollector(key, value);
          }
          continue;
        }
        if (value instanceof Number) {
          Number valueNumber = (Number) value;
          if (keyStats instanceof IntColumnPreIndexStatsCollector) {
            keyStats.collect(valueNumber.intValue());
            continue;
          }
          if (keyStats instanceof LongColumnPreIndexStatsCollector) {
            keyStats.collect(valueNumber.longValue());
            continue;
          }
          if (keyStats instanceof FloatColumnPreIndexStatsCollector) {
            keyStats.collect(valueNumber.floatValue());
            continue;
          }
          if (keyStats instanceof DoubleColumnPreIndexStatsCollector) {
            keyStats.collect(valueNumber.doubleValue());
            continue;
          }
        }
        if (keyStats instanceof IntColumnPreIndexStatsCollector) {
          try {
            keyStats.collect(Integer.parseInt(value.toString()));
          } catch (NumberFormatException e) {
            LOGGER.warn(
                "Could not parse map key '{}' value '{}' as INT; promoting to STRING collector",
                key, value);
            keyStats = promoteNumericKeyStatsToStringCollector(key, value);
          }
          continue;
        }
        if (keyStats instanceof LongColumnPreIndexStatsCollector) {
          try {
            keyStats.collect(Long.parseLong(value.toString()));
          } catch (NumberFormatException e) {
            LOGGER.warn(
                "Could not parse map key '{}' value '{}' as LONG; promoting to STRING collector",
                key, value);
            keyStats = promoteNumericKeyStatsToStringCollector(key, value);
          }
          continue;
        }
        if (keyStats instanceof FloatColumnPreIndexStatsCollector) {
          try {
            keyStats.collect(Float.parseFloat(value.toString()));
          } catch (NumberFormatException e) {
            LOGGER.warn(
                "Could not parse map key '{}' value '{}' as FLOAT; promoting to STRING collector",
                key, value);
            keyStats = promoteNumericKeyStatsToStringCollector(key, value);
          }
          continue;
        }
        if (keyStats instanceof DoubleColumnPreIndexStatsCollector) {
          try {
            keyStats.collect(Double.parseDouble(value.toString()));
          } catch (NumberFormatException e) {
            LOGGER.warn(
                "Could not parse map key '{}' value '{}' as DOUBLE; promoting to STRING collector",
                key, value);
            keyStats = promoteNumericKeyStatsToStringCollector(key, value);
          }
          continue;
        }
        // Catch all
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
      return _sortedKeys[0];
    }
    throw new IllegalStateException("you must seal the collector first before asking for min value");
  }

  @Override
  public String getMaxValue() {
    if (_sealed) {
      return _sortedKeys[_sortedKeys.length - 1];
    }
    throw new IllegalStateException("you must seal the collector first before asking for max value");
  }

  @Override
  public String[] getUniqueValuesSet() {
    if (_sealed) {
      return _sortedKeys;
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
      //All the keys which have appeared less than total docs insert default null Value in unique values
      for (Map.Entry<String, Integer> entry : _keyFrequencies.entrySet()) {
        if (entry.getValue() < _totalNumberOfEntries) {
          _keyStats.get(entry.getKey()).collect(_keyStats.get(entry.getKey())._fieldSpec.getDefaultNullValue());
        }
      }
      _sortedKeys = _keyStats.keySet().toArray(new String[0]);
      Arrays.sort(_sortedKeys);

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
    return createKeyStatsCollector(key, convertToDataType(type));
  }

  private AbstractColumnStatisticsCollector createKeyStatsCollector(String key, FieldSpec.DataType dataType) {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(key).build();
    Schema keySchema = new Schema.SchemaBuilder().setSchemaName(key)
        .addField(new DimensionFieldSpec(key, dataType, false)).build();
    StatsCollectorConfig config = new StatsCollectorConfig(tableConfig, keySchema, null);

    if (_createNoDictCollectorsForKeys) {
      return new NoDictColumnStatisticsCollector(key, config);
    }
    switch (dataType) {
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
      case BOOLEAN:
      case STRING:
      case MAP:
      case OBJECT:
        return new StringColumnPreIndexStatsCollector(key, config);
      default:
        LOGGER.warn("Unknown data type {} for key {}", dataType, key);
        return new StringColumnPreIndexStatsCollector(key, config);
      }
  }

  private AbstractColumnStatisticsCollector promoteNumericKeyStatsToStringCollector(String key, Object value) {
    Object firstRawValue = _firstRawValuesByKey.get(key);
    _firstRawValuesByKey.putIfAbsent(key, value);

    AbstractColumnStatisticsCollector stringKeyStats = createStringKeyStatsCollector(key);
    _keyStats.put(key, stringKeyStats);
    // Best-effort recovery: replay the first observed raw value as string to keep a partial history.
    if (firstRawValue != null) {
      stringKeyStats.collect(String.valueOf(firstRawValue));
    }
    stringKeyStats.collect(String.valueOf(value));
    return stringKeyStats;
  }

  private StringColumnPreIndexStatsCollector createStringKeyStatsCollector(String key) {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(key).build();
    Schema keySchema = new Schema.SchemaBuilder().setSchemaName(key)
        .addField(new DimensionFieldSpec(key, FieldSpec.DataType.STRING, false)).build();
    StatsCollectorConfig config = new StatsCollectorConfig(tableConfig, keySchema, null);
    return new StringColumnPreIndexStatsCollector(key, config);
  }

  /**
   * Convert Map value data type to stored field type.
   * Note that all unknown types are automatically converted to String type.
   */
  private static FieldSpec.DataType convertToDataType(PinotDataType ty) {
    switch (ty) {
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
      case BOOLEAN:
      case STRING:
      case OBJECT:
      case MAP:
      default:
        return FieldSpec.DataType.STRING;
    }
  }
}
