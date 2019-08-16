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
package org.apache.pinot.core.operator.transform;

import java.io.Serializable;
import org.apache.pinot.common.data.FieldSpec;
import org.apache.pinot.common.data.FieldSpec.DataType;
import org.apache.pinot.common.request.transform.TransformExpressionTree;
import org.apache.pinot.common.utils.BytesUtils;
import org.apache.pinot.common.utils.primitive.ByteArray;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.segment.index.readers.Dictionary;

public class TransformBlockDataFetcher {


  private final Fetcher[] _fetchers;

  public TransformBlockDataFetcher(BlockValSet[] blockValSets, Dictionary[] dictionaries,
      TransformResultMetadata[] expressionResultMetadata) {
    _fetchers = new Fetcher[blockValSets.length];
    for (int i = 0; i < blockValSets.length; i++) {
      _fetchers[i] = createFetcher(blockValSets[i], dictionaries[i],
          expressionResultMetadata[i]);
    }
  }

  public Serializable[] getRow(int docId) {
    Serializable[] row = new Serializable[_fetchers.length];
    for (int i = 0; i < _fetchers.length; i++) {
      row[i] = _fetchers[i].getValue(docId);
    }
    return row;
  }

  Fetcher createFetcher(BlockValSet blockValSet,
      Dictionary dictionary,
      TransformResultMetadata expressionResultMetadata) {
    if (expressionResultMetadata.hasDictionary()) {
      if (expressionResultMetadata.isSingleValue()) {
        return new DictionaryBasedSVValueFetcher(dictionary,
            blockValSet.getDictionaryIdsSV(), expressionResultMetadata.getDataType());
      } else {
        switch (expressionResultMetadata.getDataType()) {
          case INT:
            return new DictionaryBasedMVIntValueFetcher(dictionary,
                blockValSet.getDictionaryIdsMV());
          case LONG:
            return new DictionaryBasedMVLongValueFetcher(dictionary,
                blockValSet.getDictionaryIdsMV());
          case FLOAT:
            return new DictionaryBasedMVFloatValueFetcher(dictionary,
                blockValSet.getDictionaryIdsMV());
          case DOUBLE:
            return new DictionaryBasedMVDoubleValueFetcher(dictionary,
                blockValSet.getDictionaryIdsMV());
          case BOOLEAN:
          case STRING:
            return new DictionaryBasedMVStringValueFetcher(dictionary,
                blockValSet.getDictionaryIdsMV());
          case BYTES:
            return new DictionaryBasedMVBytesValueFetcher(dictionary,
                blockValSet.getDictionaryIdsMV());
        }
      }
    } else {
      switch (expressionResultMetadata.getDataType()) {
        case INT:
          return new SVIntValueFetcher(blockValSet.getIntValuesSV());
        case LONG:
          return new SVLongValueFetcher(blockValSet.getLongValuesSV());
        case FLOAT:
          return new SVFloatValueFetcher(blockValSet.getFloatValuesSV());
        case DOUBLE:
          return new SVDoubleValueFetcher(blockValSet.getDoubleValuesSV());
        case BOOLEAN:
        case STRING:
          return new SVStringValueFetcher(blockValSet.getStringValuesSV());
        case BYTES:
          return new SVBytesValueFetcher(blockValSet.getBytesValuesSV());
      }
    }
    throw new UnsupportedOperationException();
  }
}


interface Fetcher {

  Serializable getValue(int docId);
}

class SVIntValueFetcher implements Fetcher {

  private int[] _values;

  SVIntValueFetcher(int[] values) {
    _values = values;
  }

  public Serializable getValue(int docId) {
    return _values[docId];
  }
}

class SVLongValueFetcher implements Fetcher {

  private long[] _values;

  SVLongValueFetcher(long[] values) {
    _values = values;
  }

  public Serializable getValue(int docId) {
    return _values[docId];
  }
}

class SVFloatValueFetcher implements Fetcher {

  private float[] _values;

  SVFloatValueFetcher(float[] values) {
    _values = values;
  }

  public Serializable getValue(int docId) {
    return _values[docId];
  }
}

class SVDoubleValueFetcher implements Fetcher {

  private double[] _values;

  SVDoubleValueFetcher(double[] values) {
    _values = values;
  }

  public Serializable getValue(int docId) {
    return _values[docId];
  }
}

class SVStringValueFetcher implements Fetcher {

  private String[] _values;

  SVStringValueFetcher(String[] values) {
    _values = values;
  }

  public Serializable getValue(int docId) {
    return _values[docId];
  }
}

class SVBytesValueFetcher implements Fetcher {

  private byte[][] _values;

  SVBytesValueFetcher(byte[][] values) {
    _values = values;
  }

  public Serializable getValue(int docId) {
    return _values[docId];
  }
}

class DictionaryBasedSVValueFetcher implements Fetcher {

  private Dictionary _dictionary;
  private int[] _dictionaryIds;
  private DataType _dataType;

  DictionaryBasedSVValueFetcher(Dictionary dictionary, int[] dictionaryIds,
      DataType dataType) {

    _dictionary = dictionary;
    _dictionaryIds = dictionaryIds;
    _dataType = dataType;
  }

  public Serializable getValue(int docId) {
    return (Serializable) _dictionary.get(_dictionaryIds[docId]);
  }
}


class DictionaryBasedMVIntValueFetcher implements Fetcher {

  private Dictionary _dictionary;
  private int[][] _dictionaryIdsArray;

  DictionaryBasedMVIntValueFetcher(Dictionary dictionary, int[][] dictionaryIdsArray) {

    _dictionary = dictionary;
    _dictionaryIdsArray = dictionaryIdsArray;
  }

  public Serializable getValue(int docId) {
    int[] dictIds = _dictionaryIdsArray[docId];
    int[] values = new int[dictIds.length];
    for (int i = 0; i < dictIds.length; i++) {
      int dictId = dictIds[i];
      values[i] = _dictionary.getIntValue(dictId);
    }
    return values;
  }
}

class DictionaryBasedMVLongValueFetcher implements Fetcher {

  private Dictionary _dictionary;
  private int[][] _dictionaryIdsArray;

  DictionaryBasedMVLongValueFetcher(Dictionary dictionary, int[][] dictionaryIdsArray) {

    _dictionary = dictionary;
    _dictionaryIdsArray = dictionaryIdsArray;
  }

  public Serializable getValue(int docId) {
    int[] dictIds = _dictionaryIdsArray[docId];
    long[] values = new long[dictIds.length];
    for (int i = 0; i < dictIds.length; i++) {
      int dictId = dictIds[i];
      values[i] = _dictionary.getLongValue(dictId);
    }
    return values;
  }
}

class DictionaryBasedMVFloatValueFetcher implements Fetcher {

  private Dictionary _dictionary;
  private int[][] _dictionaryIdsArray;

  DictionaryBasedMVFloatValueFetcher(Dictionary dictionary, int[][] dictionaryIdsArray) {

    _dictionary = dictionary;
    _dictionaryIdsArray = dictionaryIdsArray;
  }

  public Serializable getValue(int docId) {
    int[] dictIds = _dictionaryIdsArray[docId];
    float[] values = new float[dictIds.length];
    for (int i = 0; i < dictIds.length; i++) {
      int dictId = dictIds[i];
      values[i] = _dictionary.getFloatValue(dictId);
    }
    return values;
  }
}

class DictionaryBasedMVDoubleValueFetcher implements Fetcher {

  private Dictionary _dictionary;
  private int[][] _dictionaryIdsArray;

  DictionaryBasedMVDoubleValueFetcher(Dictionary dictionary, int[][] dictionaryIdsArray) {

    _dictionary = dictionary;
    _dictionaryIdsArray = dictionaryIdsArray;
  }

  public Serializable getValue(int docId) {
    int[] dictIds = _dictionaryIdsArray[docId];
    double[] values = new double[dictIds.length];
    for (int i = 0; i < dictIds.length; i++) {
      int dictId = dictIds[i];
      values[i] = _dictionary.getDoubleValue(dictId);
    }
    return values;
  }
}

class DictionaryBasedMVStringValueFetcher implements Fetcher {

  private Dictionary _dictionary;
  private int[][] _dictionaryIdsArray;

  DictionaryBasedMVStringValueFetcher(Dictionary dictionary, int[][] dictionaryIdsArray) {

    _dictionary = dictionary;
    _dictionaryIdsArray = dictionaryIdsArray;
  }

  public Serializable getValue(int docId) {
    int[] dictIds = _dictionaryIdsArray[docId];
    String[] values = new String[dictIds.length];
    for (int i = 0; i < dictIds.length; i++) {
      int dictId = dictIds[i];
      values[i] = _dictionary.getStringValue(dictId);
    }
    return values;
  }
}

class DictionaryBasedMVBytesValueFetcher implements Fetcher {

  private Dictionary _dictionary;
  private int[][] _dictionaryIdsArray;

  DictionaryBasedMVBytesValueFetcher(Dictionary dictionary, int[][] dictionaryIdsArray) {

    _dictionary = dictionary;
    _dictionaryIdsArray = dictionaryIdsArray;
  }

  public Serializable getValue(int docId) {
    int[] dictIds = _dictionaryIdsArray[docId];
    byte[][] values = new byte[dictIds.length][];
    for (int i = 0; i < dictIds.length; i++) {
      int dictId = dictIds[i];
      values[i] = _dictionary.getBytesValue(dictId);
    }
    return values;
  }
}
