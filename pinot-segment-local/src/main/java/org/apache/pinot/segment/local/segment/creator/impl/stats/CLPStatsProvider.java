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


public interface CLPStatsProvider {

  StringColumnPreIndexStatsCollector.CLPStats getCLPStats();

  class CLPStats {
    private final EncodedMessage _clpEncodedMessage;
    private final MessageEncoder _clpMessageEncoder;
    int _totalNumberOfDictVars = 0;
    int _totalNumberOfEncodedVars = 0;
    int _maxNumberOfEncodedVars = 0;
    private String[] _sortedLogTypeValues;
    private String[] _sortedDictVarValues;
    private Set<String> _logTypes = new ObjectOpenHashSet<>(AbstractColumnStatisticsCollector.INITIAL_HASH_SET_SIZE);
    private Set<String> _dictVars = new ObjectOpenHashSet<>(AbstractColumnStatisticsCollector.INITIAL_HASH_SET_SIZE);

    public CLPStats(String[] sortedLogTypeValues, String[] sortedDictVarValues, int totalNumberOfDictVars,
        int totalNumberOfEncodedVars, int maxNumberOfEncodedVars) {
      _sortedLogTypeValues = sortedLogTypeValues;
      _sortedDictVarValues = sortedDictVarValues;
      _totalNumberOfDictVars = totalNumberOfDictVars;
      _totalNumberOfEncodedVars = totalNumberOfEncodedVars;
      _maxNumberOfEncodedVars = maxNumberOfEncodedVars;
      _clpEncodedMessage = null;
      _clpMessageEncoder = null;
    }

    public CLPStats() {
      _clpEncodedMessage = new EncodedMessage();
      _clpMessageEncoder = new MessageEncoder(BuiltInVariableHandlingRuleVersions.VariablesSchemaV2,
          BuiltInVariableHandlingRuleVersions.VariableEncodingMethodsV1);
    }

    public int getMaxNumberOfEncodedVars() {
      return _maxNumberOfEncodedVars;
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
