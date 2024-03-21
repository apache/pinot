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

import com.google.common.annotations.VisibleForTesting;
import com.yscope.clp.compressorfrontend.BuiltInVariableHandlingRuleVersions;
import com.yscope.clp.compressorfrontend.EncodedMessage;
import com.yscope.clp.compressorfrontend.MessageEncoder;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import org.apache.pinot.segment.spi.creator.StatsCollectorConfig;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.data.FieldSpec;

import static java.nio.charset.StandardCharsets.UTF_8;


public class StringColumnPreIndexStatsCollector extends AbstractColumnStatisticsCollector implements CLPStatsProvider {
  private Set<String> _values = new ObjectOpenHashSet<>(INITIAL_HASH_SET_SIZE);
  private int _minLength = Integer.MAX_VALUE;
  private int _maxLength = 0;
  private int _maxRowLength = 0;
  private String[] _sortedValues;
  private boolean _sealed = false;
  private CLPStatsCollector _clpStatsCollector;

  public StringColumnPreIndexStatsCollector(String column, StatsCollectorConfig statsCollectorConfig) {
    super(column, statsCollectorConfig);
    if (_fieldConfig != null && _fieldConfig.getCompressionCodec() == FieldConfig.CompressionCodec.CLP) {
      _clpStatsCollector = new CLPStatsCollector();
    }
  }

  @Override
  public void collect(Object entry) {
    assert !_sealed;

    if (entry instanceof Object[]) {
      Object[] values = (Object[]) entry;
      int rowLength = 0;
      for (Object obj : values) {
        String value = (String) obj;
        _values.add(value);
        if (_clpStatsCollector != null) {
          _clpStatsCollector.collect(value);
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
      if (_clpStatsCollector != null) {
        _clpStatsCollector.collect(value);
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

  @Override
  public CLPStats getCLPStats() {
    if (_sealed) {
      return _clpStatsCollector.getCLPStats();
    }
    throw new IllegalStateException("The collector must be sealed before calling getCLPStats");
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
      if (_clpStatsCollector != null) {
        _clpStatsCollector.seal();
      }
      _sealed = true;
    }
  }

  @VisibleForTesting
  public static class CLPStatsCollector {
    private final EncodedMessage _clpEncodedMessage;
    private final MessageEncoder _clpMessageEncoder;
    int _totalNumberOfDictVars = 0;
    int _totalNumberOfEncodedVars = 0;
    int _maxNumberOfEncodedVars = 0;
    private Set<String> _logTypes = new ObjectOpenHashSet<>(AbstractColumnStatisticsCollector.INITIAL_HASH_SET_SIZE);
    private Set<String> _dictVars = new ObjectOpenHashSet<>(AbstractColumnStatisticsCollector.INITIAL_HASH_SET_SIZE);
    private CLPStats _clpStats;

    public CLPStatsCollector() {
      _clpEncodedMessage = new EncodedMessage();
      _clpMessageEncoder = new MessageEncoder(BuiltInVariableHandlingRuleVersions.VariablesSchemaV2,
          BuiltInVariableHandlingRuleVersions.VariableEncodingMethodsV1);
    }

    public void collect(String value) {
      String logType;
      String[] dictVars;
      Long[] encodedVars;

      try {
        _clpMessageEncoder.encodeMessage(value, _clpEncodedMessage);
        logType = _clpEncodedMessage.getLogTypeAsString();
        dictVars = _clpEncodedMessage.getDictionaryVarsAsStrings();
        encodedVars = _clpEncodedMessage.getEncodedVarsAsBoxedLongs();
      } catch (IOException e) {
        throw new IllegalArgumentException("Failed to encode message: " + value, e);
      }

      if (logType == null) {
        logType = FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_STRING;
      }

      if (dictVars == null) {
        dictVars = new String[]{FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_STRING};
      }

      if (encodedVars == null) {
        encodedVars = new Long[]{FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_LONG};
      }

      _logTypes.add(logType);
      _dictVars.addAll(Arrays.asList(dictVars));
      _totalNumberOfDictVars += dictVars.length;
      _totalNumberOfEncodedVars += encodedVars.length;
      _maxNumberOfEncodedVars = Math.max(_maxNumberOfEncodedVars, encodedVars.length);
    }

    public void seal() {
      String[] sortedLogTypeValues = _logTypes.toArray(new String[0]);
      _logTypes = null;
      Arrays.sort(sortedLogTypeValues);
      String[] sortedDictVarValues = _dictVars.toArray(new String[0]);
      _dictVars = null;
      Arrays.sort(sortedDictVarValues);
      _clpStats =
          new CLPStats(sortedLogTypeValues, sortedDictVarValues, _totalNumberOfDictVars, _totalNumberOfEncodedVars,
              _maxNumberOfEncodedVars);
    }

    public CLPStats getCLPStats() {
      return _clpStats;
    }
  }
}
