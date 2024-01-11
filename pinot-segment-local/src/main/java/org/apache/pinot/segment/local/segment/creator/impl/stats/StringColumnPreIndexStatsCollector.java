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

import com.yscope.clp.compressorfrontend.BuiltInVariableHandlingRuleVersions;
import com.yscope.clp.compressorfrontend.EncodedMessage;
import com.yscope.clp.compressorfrontend.MessageEncoder;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import org.apache.pinot.segment.spi.creator.StatsCollectorConfig;
import org.apache.pinot.spi.config.table.FieldConfig;

import static java.nio.charset.StandardCharsets.UTF_8;


public class StringColumnPreIndexStatsCollector extends AbstractColumnStatisticsCollector {
  public StringColumnPreIndexStatsCollector(String column, StatsCollectorConfig statsCollectorConfig) {
    super(column, statsCollectorConfig);
    if (_fieldConfig != null && _fieldConfig.getCompressionCodec() == FieldConfig.CompressionCodec.CLP) {
      _clpStats = new CLPStats();
    }
  }
  private Set<String> _values = new ObjectOpenHashSet<>(INITIAL_HASH_SET_SIZE);
  private int _minLength = Integer.MAX_VALUE;
  private int _maxLength = 0;
  private int _maxRowLength = 0;
  private String[] _sortedValues;
  private boolean _sealed = false;
  private CLPStats _clpStats;

  @Override
  public void collect(Object entry) {
    assert !_sealed;

    if (entry instanceof Object[]) {
      Object[] values = (Object[]) entry;
      int rowLength = 0;
      for (Object obj : values) {
        String value = (String) obj;
        _values.add(value);
        if (_clpStats != null) {
          _clpStats.collect(value);
        }

        int length = value.getBytes(UTF_8).length;
        _minLength = Math.min(_minLength, length);
        _maxLength = Math.max(_maxLength, length);
        rowLength += length;
      }

      _maxNumberOfMultiValues = Math.max(_maxNumberOfMultiValues, values.length);
      _maxRowLength = Math.max(_maxRowLength, rowLength);
      updateTotalNumberOfEntries(values);
    } else {
      String value = (String) entry;
      addressSorted(value);
      if (_clpStats != null) {
        _clpStats.collect(value);
      }
      if (_values.add(value)) {
        if (isPartitionEnabled()) {
          updatePartition(value);
        }
        int valueLength = value.getBytes(UTF_8).length;
        _minLength = Math.min(_minLength, valueLength);
        _maxLength = Math.max(_maxLength, valueLength);
        _maxRowLength = _maxLength;
      }
      _totalNumberOfEntries++;
    }
  }

  public CLPStats getClpStats() {
    return _clpStats;
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
  public Object[] getUniqueValuesSet() {
    if (_sealed) {
      return _sortedValues;
    }
    throw new IllegalStateException("you must seal the collector first before asking for unique values set");
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
      _sortedValues = _values.toArray(new String[0]);
      _values = null;
      Arrays.sort(_sortedValues);
      if (_clpStats != null) {
        _clpStats.seal();
      }
      _sealed = true;
    }
  }

  public static class CLPStats {
    int _totalNumberOfDictVars = 0;
    private String[] _sortedLogTypeValues;
    int _totalNumberOfEncodedVars = 0;
    private String[] _sortedDictVarValues;

    public int getMaxNumberOfEncodedVars() {
      return _maxNumberOfEncodedVars;
    }

    int _maxNumberOfEncodedVars = 0;
    private Set<String> _logTypes = new ObjectOpenHashSet<>(INITIAL_HASH_SET_SIZE);
    private Set<String> _dictVars = new ObjectOpenHashSet<>(INITIAL_HASH_SET_SIZE);
    private final EncodedMessage _clpEncodedMessage;
    private final MessageEncoder _clpMessageEncoder;
    public CLPStats() {
      _clpEncodedMessage = new EncodedMessage();
      _clpMessageEncoder = new MessageEncoder(BuiltInVariableHandlingRuleVersions.VariablesSchemaV2,
          BuiltInVariableHandlingRuleVersions.VariableEncodingMethodsV1);
    }

    public int getTotalNumberOfDictVars() {
      return _totalNumberOfDictVars;
    }

    public int getTotalNumberOfEncodedVars() {
      return _totalNumberOfEncodedVars;
    }

    public void collect(String value) {
      String logType;
      String[] dictVars;
      try {
        _clpMessageEncoder.encodeMessage(value, _clpEncodedMessage);
        logType = _clpEncodedMessage.getLogTypeAsString();
        dictVars = _clpEncodedMessage.getDictionaryVarsAsStrings();
      } catch (IOException e) {
        throw new IllegalArgumentException("Failed to encode message: " + value, e);
      }
      _logTypes.add(logType);
      _dictVars.addAll(Arrays.asList(dictVars));
      _totalNumberOfDictVars += dictVars.length;
      _totalNumberOfEncodedVars += _clpEncodedMessage.getEncodedVarsAsBoxedLongs().length;
      _maxNumberOfEncodedVars =
          Math.max(_maxNumberOfEncodedVars, _clpEncodedMessage.getEncodedVarsAsBoxedLongs().length);
    }

    public void seal() {
      _sortedLogTypeValues = _logTypes.toArray(new String[0]);
      _logTypes = null;
      Arrays.sort(_sortedLogTypeValues);
      _sortedDictVarValues = _dictVars.toArray(new String[0]);
      _dictVars = null;
      Arrays.sort(_sortedDictVarValues);
    }

    public void clear() {
      _sortedLogTypeValues = null;
      _sortedDictVarValues = null;
    }

    public String[] getSortedLogTypeValues() {
      return _sortedLogTypeValues;
    }

    public String[] getSortedDictVarValues() {
      return _sortedDictVarValues;
    }
  }
}
