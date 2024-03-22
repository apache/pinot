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
package org.apache.pinot.segment.local.realtime.impl.forward;

import com.yscope.clp.compressorfrontend.BuiltInVariableHandlingRuleVersions;
import com.yscope.clp.compressorfrontend.EncodedMessage;
import com.yscope.clp.compressorfrontend.MessageDecoder;
import com.yscope.clp.compressorfrontend.MessageEncoder;
import java.io.IOException;
import org.apache.pinot.segment.local.realtime.impl.dictionary.StringOffHeapMutableDictionary;
import org.apache.pinot.segment.local.segment.creator.impl.stats.CLPStatsProvider;
import org.apache.pinot.segment.local.segment.creator.impl.stats.StringColumnPreIndexStatsCollector;
import org.apache.pinot.segment.local.segment.index.forward.ForwardIndexType;
import org.apache.pinot.segment.spi.index.mutable.MutableDictionary;
import org.apache.pinot.segment.spi.index.mutable.MutableForwardIndex;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.apache.pinot.spi.data.FieldSpec;


public class CLPMutableForwardIndex implements MutableForwardIndex {
  // TODO: We can get better dynamic estimates using segment stats
  private static final int ESTIMATED_LOG_TYPE_CARDINALITY = 10000;
  private static final int ESTIMATED_DICT_VARS_CARDINALITY = 10000;
  private static final int ESTIMATED_LOG_TYPE_LENGTH = 200;
  private static final int ESTIMATED_DICT_VARS_LENGTH = 50;
  private FieldSpec.DataType _storedType;
  private final EncodedMessage _clpEncodedMessage;
  private final MessageEncoder _clpMessageEncoder;
  private final MessageDecoder _clpMessageDecoder;
  private final MutableDictionary _logTypeDictCreator;
  private final MutableDictionary _dictVarsDictCreator;
  private final FixedByteSVMutableForwardIndex _logTypeFwdIndex;
  private final FixedByteMVMutableForwardIndex _dictVarsFwdIndex;
  private final FixedByteMVMutableForwardIndex _encodedVarsFwdIndex;

  // clp stats
  int _totalNumberOfDictVars = 0;
  int _maxNumberOfEncodedVars = 0;
  int _totalNumberOfEncodedVars = 0;
  private int _lengthOfShortestElement;
  private int _lengthOfLongestElement;

  public CLPMutableForwardIndex(String columnName, FieldSpec.DataType storedType,
      PinotDataBufferMemoryManager memoryManager, int capacity) {
    _clpEncodedMessage = new EncodedMessage();
    _clpMessageEncoder = new MessageEncoder(BuiltInVariableHandlingRuleVersions.VariablesSchemaV2,
        BuiltInVariableHandlingRuleVersions.VariableEncodingMethodsV1);
    _logTypeDictCreator =
        new StringOffHeapMutableDictionary(ESTIMATED_LOG_TYPE_CARDINALITY, ESTIMATED_LOG_TYPE_CARDINALITY / 10,
            memoryManager, columnName + "_logType.dict", ESTIMATED_LOG_TYPE_LENGTH);
    _dictVarsDictCreator =
        new StringOffHeapMutableDictionary(ESTIMATED_DICT_VARS_CARDINALITY, ESTIMATED_DICT_VARS_CARDINALITY / 10,
            memoryManager, columnName + "_dictVars.dict", ESTIMATED_DICT_VARS_LENGTH);

    _logTypeFwdIndex = new FixedByteSVMutableForwardIndex(true, FieldSpec.DataType.INT, capacity, memoryManager,
        columnName + "_logType.fwd");
    _dictVarsFwdIndex =
        new FixedByteMVMutableForwardIndex(ForwardIndexType.MAX_MULTI_VALUES_PER_ROW, 20, capacity, Integer.BYTES,
            memoryManager, columnName + "_dictVars.fwd", true, FieldSpec.DataType.INT);
    _encodedVarsFwdIndex =
        new FixedByteMVMutableForwardIndex(ForwardIndexType.MAX_MULTI_VALUES_PER_ROW, 20, capacity, Long.BYTES,
            memoryManager, columnName + "_encodedVars.fwd", true, FieldSpec.DataType.LONG);
    _clpMessageDecoder = new MessageDecoder(BuiltInVariableHandlingRuleVersions.VariablesSchemaV2,
        BuiltInVariableHandlingRuleVersions.VariableEncodingMethodsV1);
    _storedType = storedType;
  }

  @Override
  public int getLengthOfShortestElement() {
    return _lengthOfShortestElement;
  }

  @Override
  public int getLengthOfLongestElement() {
    return _lengthOfLongestElement;
  }

  @Override
  public boolean isDictionaryEncoded() {
    return false;
  }

  @Override
  public boolean isSingleValue() {
    return true;
  }

  @Override
  public FieldSpec.DataType getStoredType() {
    return _storedType;
  }

  @Override
  public void setString(int docId, String value) {
    String logtype;
    String[] dictVars;
    Long[] encodedVars;

    _lengthOfLongestElement = Math.max(_lengthOfLongestElement, value.length());
    _lengthOfShortestElement = Math.min(_lengthOfShortestElement, value.length());

    try {
      _clpMessageEncoder.encodeMessage(value, _clpEncodedMessage);
      logtype = _clpEncodedMessage.getLogTypeAsString();
      dictVars = _clpEncodedMessage.getDictionaryVarsAsStrings();
      encodedVars = _clpEncodedMessage.getEncodedVarsAsBoxedLongs();
    } catch (IOException e) {
      throw new IllegalArgumentException("Failed to encode message: " + value, e);
    }

    _totalNumberOfDictVars += dictVars.length;
    _totalNumberOfEncodedVars += encodedVars.length;
    _maxNumberOfEncodedVars = Math.max(_maxNumberOfEncodedVars, encodedVars.length);

    int logTypeDictId = _logTypeDictCreator.index(logtype);
    _logTypeFwdIndex.setDictId(docId, logTypeDictId);

    int[] dictVarsDictIds = new int[dictVars.length];
    for (int i = 0; i < dictVars.length; i++) {
      dictVarsDictIds[i] = _dictVarsDictCreator.index(dictVars[i]);
    }
    _dictVarsFwdIndex.setDictIdMV(docId, dictVarsDictIds);

    long[] encodedVarsLongs = new long[encodedVars.length];
    for (int i = 0; i < encodedVars.length; i++) {
      encodedVarsLongs[i] = encodedVars[i];
    }
    _encodedVarsFwdIndex.setLongMV(docId, encodedVarsLongs);
  }

  @Override
  public String getString(int docId) {
    String logType = _logTypeDictCreator.getStringValue(_logTypeFwdIndex.getDictId(docId));
    int[] dictVarsDictIds = _dictVarsFwdIndex.getDictIdMV(docId);
    String[] dictVars = new String[dictVarsDictIds.length];
    for (int i = 0; i < dictVarsDictIds.length; i++) {
      dictVars[i] = _dictVarsDictCreator.getStringValue(dictVarsDictIds[i]);
    }
    long[] encodedVarsLongs = _encodedVarsFwdIndex.getLongMV(docId);
    try {
      return _clpMessageDecoder.decodeMessage(logType, dictVars, encodedVarsLongs);
    } catch (IOException e) {
      throw new IllegalArgumentException("Failed to encode message: " + logType, e);
    }
  }

  public StringColumnPreIndexStatsCollector.CLPStats getCLPStats() {
    return new CLPStatsProvider.CLPStats((String[]) _logTypeDictCreator.getSortedValues(),
        (String[]) _dictVarsDictCreator.getSortedValues(), _totalNumberOfDictVars, _totalNumberOfEncodedVars,
        _maxNumberOfEncodedVars);
  }

  @Override
  public void close()
      throws IOException {
    _logTypeDictCreator.close();
    _dictVarsDictCreator.close();
    _logTypeFwdIndex.close();
    _dictVarsFwdIndex.close();
    _encodedVarsFwdIndex.close();
  }
}
