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
import org.apache.pinot.spi.data.FieldSpec;


public interface CLPStatsProvider {

  CLPStats getCLPStats();

  class CLPStats {
    int _totalNumberOfDictVars = 0;
    int _totalNumberOfEncodedVars = 0;
    int _maxNumberOfEncodedVars = 0;
    private String[] _sortedLogTypeValues;
    private String[] _sortedDictVarValues;

    public CLPStats(String[] sortedLogTypeValues, String[] sortedDictVarValues, int totalNumberOfDictVars,
        int totalNumberOfEncodedVars, int maxNumberOfEncodedVars) {
      _sortedLogTypeValues = sortedLogTypeValues;
      _sortedDictVarValues = sortedDictVarValues;
      _totalNumberOfDictVars = totalNumberOfDictVars;
      _totalNumberOfEncodedVars = totalNumberOfEncodedVars;
      _maxNumberOfEncodedVars = maxNumberOfEncodedVars;
    }

    public void clear() {
      _sortedLogTypeValues = null;
      _sortedDictVarValues = null;
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

    public String[] getSortedLogTypeValues() {
      return _sortedLogTypeValues;
    }

    public String[] getSortedDictVarValues() {
      return _sortedDictVarValues;
    }
  }
}
