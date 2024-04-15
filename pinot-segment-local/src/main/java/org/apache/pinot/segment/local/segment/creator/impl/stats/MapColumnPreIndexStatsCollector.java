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

import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.common.utils.PinotDataType;
import org.apache.pinot.segment.spi.creator.StatsCollectorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
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
 * heterogenous value types for a key are encountered will constructing the Map statistics it can be raised as a
 * fault.
 */
public class MapColumnPreIndexStatsCollector extends AbstractColumnStatisticsCollector {
  private ObjectOpenHashSet<String> _keys = new ObjectOpenHashSet<>(INITIAL_HASH_SET_SIZE);
  private final HashMap<String, AbstractColumnStatisticsCollector> _keyStats;
  private String[] _sortedValues;
  private int _minLength = Integer.MAX_VALUE;
  private int _maxLength = 0;
  private int _maxRowLength = 0;
  private String _minValue = null;
  private String _maxValue = null;
  private boolean _sealed = false;
  private final String _column;

  public MapColumnPreIndexStatsCollector(String column, StatsCollectorConfig statsCollectorConfig) {
    super(column, statsCollectorConfig);
    super._sorted = false;
    _keyStats = new HashMap<>();
    _column = column;
  }

  public AbstractColumnStatisticsCollector getKeyStatistics(String key) {
    return _keyStats.get(key);
  }

  @Override
  public void collect(Object entry) {
    assert !_sealed;

    if (entry instanceof Map) {
      final Map<String, Object> mapValue = (Map<String, Object>) entry;

      for (Map.Entry<String, Object> mapValueEntry : mapValue.entrySet()) {
        final String key = mapValueEntry.getKey();

        // Record statistics about the key
        int length = key.length() * Character.BYTES;
        if (_keys.add(key)) {
          if (isPartitionEnabled()) {
            updatePartition(key);
          }
          if (_minValue == null) {
            _minValue = key;
          } else {
            if (key.compareTo(_minValue) < 0) {
              _minValue = key;
            }
          }
          if (_maxValue == null) {
            _maxValue = key;
          } else {
            if (key.compareTo(_maxValue) > 0) {
              _maxValue = key;
            }
          }
          _minLength = Math.min(_minLength, length);
          _maxLength = Math.max(_maxLength, length);
          _maxRowLength = _maxLength;
        }

        // Record statistics about the value within the key
        AbstractColumnStatisticsCollector keyStats = getOrCreateKeyStatsCollector(key, mapValueEntry.getValue());
        keyStats.collect(mapValueEntry.getValue());
      }
      _totalNumberOfEntries++;
    } else {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public String getMinValue() {
    if (_sealed) {
      return _minValue;
    }
    throw new IllegalStateException("you must seal the collector first before asking for min value");
  }

  @Override
  public String getMaxValue() {
    if (_sealed) {
      return _maxValue;
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
    if (_sealed) {
      return _maxLength;
    }
    throw new IllegalStateException("you must seal the collector first before asking for longest value");
  }

  @Override
  public int getMaxRowLengthInBytes() {
    return _maxRowLength;
  }

  @Override
  public int getCardinality() {
    if (_sealed) {
      return _sortedValues.length;
    }
    throw new IllegalStateException("you must seal the collector first before asking for cardinality");
  }

  @Override
  public void seal() {
    if (!_sealed) {
      _sortedValues = _keys.stream().sorted().toArray(String[]::new);
      _keys = null;

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
   *
   * @param key
   * @param value
   * @return
   */
  private AbstractColumnStatisticsCollector getOrCreateKeyStatsCollector(String key, Object value) {
    // Check if the key stats collector exists just return it
    if (!_keyStats.containsKey(key)) {
      // Get the type of the value
      PinotDataType type = PinotDataType.getSingleValueType(value.getClass());
      TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
          .setTableName(_column)
          .build();
      Schema keySchema = new Schema.SchemaBuilder()
          .setSchemaName(key)
          .addField(new DimensionFieldSpec(key, convertToDataType(type), false))
          .build();
      StatsCollectorConfig config = new StatsCollectorConfig(tableConfig, keySchema, null);

      AbstractColumnStatisticsCollector keyStatsCollector = null;
      switch (type) {
        case INTEGER:
          keyStatsCollector = new IntColumnPreIndexStatsCollector(key, config);
          break;
        case LONG:
          keyStatsCollector = new LongColumnPreIndexStatsCollector(key, config);
          break;
        case FLOAT:
          keyStatsCollector = new FloatColumnPreIndexStatsCollector(key, config);
          break;
        case DOUBLE:
          keyStatsCollector = new DoubleColumnPreIndexStatsCollector(key, config);
          break;
        case BIG_DECIMAL:
          keyStatsCollector = new BigDecimalColumnPreIndexStatsCollector(key, config);
          break;
        case STRING:
          keyStatsCollector = new StringColumnPreIndexStatsCollector(key, config);
          break;
        case TIMESTAMP:
        case BOOLEAN:
        case BYTE:
        case CHARACTER:
        case SHORT:
        case JSON:
        case BYTES:
        case OBJECT:
        case MAP:
        case BYTE_ARRAY:
        case CHARACTER_ARRAY:
        case SHORT_ARRAY:
        case PRIMITIVE_INT_ARRAY:
        case INTEGER_ARRAY:
        case PRIMITIVE_LONG_ARRAY:
        case LONG_ARRAY:
        case PRIMITIVE_FLOAT_ARRAY:
        case FLOAT_ARRAY:
        case PRIMITIVE_DOUBLE_ARRAY:
        case DOUBLE_ARRAY:
        case BOOLEAN_ARRAY:
        case TIMESTAMP_ARRAY:
        case STRING_ARRAY:
        case BYTES_ARRAY:
        case COLLECTION:
        case OBJECT_ARRAY:
        default:
          throw new UnsupportedOperationException(String.format("MAP column does not yet support '%s'", type));
      }

      _keyStats.put(key, keyStatsCollector);
    }

    return _keyStats.get(key);
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
      case STRING:
        return FieldSpec.DataType.STRING;
      default:
        throw new UnsupportedOperationException();
    }
  }
}
