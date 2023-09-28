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
package org.apache.pinot.core.common.evaluators;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.InvalidPathException;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.ParseContext;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import java.math.BigDecimal;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.function.JsonPathCache;
import org.apache.pinot.segment.spi.evaluator.json.JsonPathEvaluator;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.readers.Vector;
import org.apache.pinot.spi.utils.JsonUtils;


public final class DefaultJsonPathEvaluator implements JsonPathEvaluator {

  // This ObjectMapper requires special configurations, hence we can't use pinot JsonUtils here.
  private static final ObjectMapper OBJECT_MAPPER_WITH_BIG_DECIMAL =
      new ObjectMapper().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);

  private static final ParseContext JSON_PARSER_CONTEXT = JsonPath.using(
      new Configuration.ConfigurationBuilder().jsonProvider(new JacksonJsonProvider())
          .mappingProvider(new JacksonMappingProvider()).options(Option.SUPPRESS_EXCEPTIONS).build());

  private static final ParseContext JSON_PARSER_CONTEXT_WITH_BIG_DECIMAL = JsonPath.using(
      new Configuration.ConfigurationBuilder().jsonProvider(new JacksonJsonProvider(OBJECT_MAPPER_WITH_BIG_DECIMAL))
          .mappingProvider(new JacksonMappingProvider()).options(Option.SUPPRESS_EXCEPTIONS).build());

  private static final int[] EMPTY_INTS = new int[0];
  private static final long[] EMPTY_LONGS = new long[0];
  private static final float[] EMPTY_FLOATS = new float[0];
  private static final double[] EMPTY_DOUBLES = new double[0];
  private static final String[] EMPTY_STRINGS = new String[0];

  public static JsonPathEvaluator create(String jsonPath, @Nullable Object defaultValue) {
    try {
      return new DefaultJsonPathEvaluator(JsonPathCache.INSTANCE.getOrCompute(jsonPath), defaultValue);
    } catch (InvalidPathException e) {
      throw new IllegalArgumentException(e);
    }
  }

  private final JsonPath _jsonPath;
  private final Object _defaultValue;

  private DefaultJsonPathEvaluator(JsonPath jsonPath, @Nullable Object defaultValue) {
    _jsonPath = jsonPath;
    _defaultValue = defaultValue;
  }

  public <T extends ForwardIndexReaderContext> void evaluateBlock(int[] docIds, int length,
      ForwardIndexReader<T> reader, T context, Dictionary dictionary, int[] dictIdsBuffer, int[] valueBuffer) {
    int defaultValue = (_defaultValue instanceof Number) ? ((Number) _defaultValue).intValue() : 0;
    if (reader.isDictionaryEncoded()) {
      reader.readDictIds(docIds, length, dictIdsBuffer, context);
      if (dictionary.getValueType() == FieldSpec.DataType.BYTES) {
        for (int i = 0; i < length; i++) {
          processValue(i, extractFromBytes(dictionary, dictIdsBuffer[i]), defaultValue, valueBuffer);
        }
      } else {
        for (int i = 0; i < length; i++) {
          processValue(i, extractFromString(dictionary, dictIdsBuffer[i]), defaultValue, valueBuffer);
        }
      }
    } else {
      switch (reader.getStoredType()) {
        case STRING:
          for (int i = 0; i < length; i++) {
            processValue(i, extractFromString(reader, context, docIds[i]), defaultValue, valueBuffer);
          }
          break;
        case BYTES:
          for (int i = 0; i < length; i++) {
            processValue(i, extractFromBytes(reader, context, docIds[i]), defaultValue, valueBuffer);
          }
          break;
        default:
          throw new IllegalStateException();
      }
    }
  }

  public <T extends ForwardIndexReaderContext> void evaluateBlock(int[] docIds, int length,
      ForwardIndexReader<T> reader, T context, Dictionary dictionary, int[] dictIdsBuffer, long[] valueBuffer) {
    long defaultValue = (_defaultValue instanceof Number) ? ((Number) _defaultValue).longValue() : 0L;
    if (reader.isDictionaryEncoded()) {
      reader.readDictIds(docIds, length, dictIdsBuffer, context);
      if (dictionary.getValueType() == FieldSpec.DataType.BYTES) {
        for (int i = 0; i < length; i++) {
          processValue(i, extractFromBytes(dictionary, dictIdsBuffer[i]), defaultValue, valueBuffer);
        }
      } else {
        for (int i = 0; i < length; i++) {
          processValue(i, extractFromString(dictionary, dictIdsBuffer[i]), defaultValue, valueBuffer);
        }
      }
    } else {
      switch (reader.getStoredType()) {
        case STRING:
          for (int i = 0; i < length; i++) {
            processValue(i, extractFromString(reader, context, docIds[i]), defaultValue, valueBuffer);
          }
          break;
        case BYTES:
          for (int i = 0; i < length; i++) {
            processValue(i, extractFromBytes(reader, context, docIds[i]), defaultValue, valueBuffer);
          }
          break;
        default:
          throw new IllegalStateException();
      }
    }
  }

  public <T extends ForwardIndexReaderContext> void evaluateBlock(int[] docIds, int length,
      ForwardIndexReader<T> reader, T context, Dictionary dictionary, int[] dictIdsBuffer, float[] valueBuffer) {
    float defaultValue = (_defaultValue instanceof Number) ? ((Number) _defaultValue).floatValue() : 0F;
    if (reader.isDictionaryEncoded()) {
      reader.readDictIds(docIds, length, dictIdsBuffer, context);
      if (dictionary.getValueType() == FieldSpec.DataType.BYTES) {
        for (int i = 0; i < length; i++) {
          processValue(i, extractFromBytes(dictionary, dictIdsBuffer[i]), defaultValue, valueBuffer);
        }
      } else {
        for (int i = 0; i < length; i++) {
          processValue(i, extractFromString(dictionary, dictIdsBuffer[i]), defaultValue, valueBuffer);
        }
      }
    } else {
      switch (reader.getStoredType()) {
        case STRING:
          for (int i = 0; i < length; i++) {
            processValue(i, extractFromString(reader, context, docIds[i]), defaultValue, valueBuffer);
          }
          break;
        case BYTES:
          for (int i = 0; i < length; i++) {
            processValue(i, extractFromBytes(reader, context, docIds[i]), defaultValue, valueBuffer);
          }
          break;
        default:
          throw new IllegalStateException();
      }
    }
  }

  public <T extends ForwardIndexReaderContext> void evaluateBlock(int[] docIds, int length,
      ForwardIndexReader<T> reader, T context, Dictionary dictionary, int[] dictIdsBuffer, double[] valueBuffer) {
    double defaultValue = (_defaultValue instanceof Number) ? ((Number) _defaultValue).doubleValue() : 0D;
    if (reader.isDictionaryEncoded()) {
      reader.readDictIds(docIds, length, dictIdsBuffer, context);
      if (dictionary.getValueType() == FieldSpec.DataType.BYTES) {
        for (int i = 0; i < length; i++) {
          processValue(i, extractFromBytes(dictionary, dictIdsBuffer[i]), defaultValue, valueBuffer);
        }
      } else {
        for (int i = 0; i < length; i++) {
          processValue(i, extractFromString(dictionary, dictIdsBuffer[i]), defaultValue, valueBuffer);
        }
      }
    } else {
      switch (reader.getStoredType()) {
        case STRING:
          for (int i = 0; i < length; i++) {
            processValue(i, extractFromString(reader, context, docIds[i]), defaultValue, valueBuffer);
          }
          break;
        case BYTES:
          for (int i = 0; i < length; i++) {
            processValue(i, extractFromBytes(reader, context, docIds[i]), defaultValue, valueBuffer);
          }
          break;
        default:
          throw new IllegalStateException();
      }
    }
  }

  public <T extends ForwardIndexReaderContext> void evaluateBlock(int[] docIds, int length,
      ForwardIndexReader<T> reader, T context, Dictionary dictionary, int[] dictIdsBuffer, BigDecimal[] valueBuffer) {
    BigDecimal defaultValue = (_defaultValue instanceof BigDecimal) ? ((BigDecimal) _defaultValue) : BigDecimal.ZERO;
    if (reader.isDictionaryEncoded()) {
      reader.readDictIds(docIds, length, dictIdsBuffer, context);
      if (dictionary.getValueType() == FieldSpec.DataType.BYTES) {
        for (int i = 0; i < length; i++) {
          processValue(i, extractFromBytesWithExactBigDecimal(dictionary, dictIdsBuffer[i]), defaultValue, valueBuffer);
        }
      } else {
        for (int i = 0; i < length; i++) {
          processValue(i, extractFromStringWithExactBigDecimal(dictionary, dictIdsBuffer[i]), defaultValue,
              valueBuffer);
        }
      }
    } else {
      switch (reader.getStoredType()) {
        case STRING:
          for (int i = 0; i < length; i++) {
            processValue(i, extractFromStringWithExactBigDecimal(reader, context, docIds[i]), defaultValue,
                valueBuffer);
          }
          break;
        case BYTES:
          for (int i = 0; i < length; i++) {
            processValue(i, extractFromBytesWithExactBigDecimal(reader, context, docIds[i]), defaultValue, valueBuffer);
          }
          break;
        default:
          throw new IllegalStateException();
      }
    }
  }

  public <T extends ForwardIndexReaderContext> void evaluateBlock(int[] docIds, int length,
      ForwardIndexReader<T> reader, T context, Dictionary dictionary, int[] dictIdsBuffer, String[] valueBuffer) {
    if (reader.isDictionaryEncoded()) {
      reader.readDictIds(docIds, length, dictIdsBuffer, context);
      if (dictionary.getValueType() == FieldSpec.DataType.BYTES) {
        for (int i = 0; i < length; i++) {
          processValue(i, extractFromBytes(dictionary, dictIdsBuffer[i]), valueBuffer);
        }
      } else {
        for (int i = 0; i < length; i++) {
          processValue(i, extractFromString(dictionary, dictIdsBuffer[i]), valueBuffer);
        }
      }
    } else {
      switch (reader.getStoredType()) {
        case STRING:
          for (int i = 0; i < length; i++) {
            processValue(i, extractFromString(reader, context, docIds[i]), valueBuffer);
          }
          break;
        case BYTES:
          for (int i = 0; i < length; i++) {
            processValue(i, extractFromBytes(reader, context, docIds[i]), valueBuffer);
          }
          break;
        default:
          throw new IllegalStateException();
      }
    }
  }

  public <T extends ForwardIndexReaderContext> void evaluateBlock(int[] docIds, int length,
      ForwardIndexReader<T> reader, T context, Dictionary dictionary, int[] dictIdsBuffer, int[][] valueBuffer) {
    if (reader.isDictionaryEncoded()) {
      reader.readDictIds(docIds, length, dictIdsBuffer, context);
      if (dictionary.getValueType() == FieldSpec.DataType.BYTES) {
        for (int i = 0; i < length; i++) {
          processList(i, extractFromBytes(dictionary, dictIdsBuffer[i]), valueBuffer);
        }
      } else {
        for (int i = 0; i < length; i++) {
          processList(i, extractFromString(dictionary, dictIdsBuffer[i]), valueBuffer);
        }
      }
    } else {
      switch (reader.getStoredType()) {
        case STRING:
          for (int i = 0; i < length; i++) {
            processList(i, extractFromString(reader, context, docIds[i]), valueBuffer);
          }
          break;
        case BYTES:
          for (int i = 0; i < length; i++) {
            processList(i, extractFromBytes(reader, context, docIds[i]), valueBuffer);
          }
          break;
        default:
          throw new IllegalStateException();
      }
    }
  }

  public <T extends ForwardIndexReaderContext> void evaluateBlock(int[] docIds, int length,
      ForwardIndexReader<T> reader, T context, Dictionary dictionary, int[] dictIdsBuffer, long[][] valueBuffer) {
    if (reader.isDictionaryEncoded()) {
      reader.readDictIds(docIds, length, dictIdsBuffer, context);
      if (dictionary.getValueType() == FieldSpec.DataType.BYTES) {
        for (int i = 0; i < length; i++) {
          processList(i, extractFromBytes(dictionary, dictIdsBuffer[i]), valueBuffer);
        }
      } else {
        for (int i = 0; i < length; i++) {
          processList(i, extractFromString(dictionary, dictIdsBuffer[i]), valueBuffer);
        }
      }
    } else {
      switch (reader.getStoredType()) {
        case STRING:
          for (int i = 0; i < length; i++) {
            processList(i, extractFromString(reader, context, docIds[i]), valueBuffer);
          }
          break;
        case BYTES:
          for (int i = 0; i < length; i++) {
            processList(i, extractFromBytes(reader, context, docIds[i]), valueBuffer);
          }
          break;
        default:
          throw new IllegalStateException();
      }
    }
  }

  public <T extends ForwardIndexReaderContext> void evaluateBlock(int[] docIds, int length,
      ForwardIndexReader<T> reader, T context, Dictionary dictionary, int[] dictIdsBuffer, float[][] valueBuffer) {
    if (reader.isDictionaryEncoded()) {
      reader.readDictIds(docIds, length, dictIdsBuffer, context);
      if (dictionary.getValueType() == FieldSpec.DataType.BYTES) {
        for (int i = 0; i < length; i++) {
          processList(i, extractFromBytes(dictionary, dictIdsBuffer[i]), valueBuffer);
        }
      } else {
        for (int i = 0; i < length; i++) {
          processList(i, extractFromString(dictionary, dictIdsBuffer[i]), valueBuffer);
        }
      }
    } else {
      switch (reader.getStoredType()) {
        case STRING:
          for (int i = 0; i < length; i++) {
            processList(i, extractFromString(reader, context, docIds[i]), valueBuffer);
          }
          break;
        case BYTES:
          for (int i = 0; i < length; i++) {
            processList(i, extractFromBytes(reader, context, docIds[i]), valueBuffer);
          }
          break;
        default:
          throw new IllegalStateException();
      }
    }
  }

  public <T extends ForwardIndexReaderContext> void evaluateBlock(int[] docIds, int length,
      ForwardIndexReader<T> reader, T context, Dictionary dictionary, int[] dictIdsBuffer, double[][] valueBuffer) {
    if (reader.isDictionaryEncoded()) {
      reader.readDictIds(docIds, length, dictIdsBuffer, context);
      if (dictionary.getValueType() == FieldSpec.DataType.BYTES) {
        for (int i = 0; i < length; i++) {
          processList(i, extractFromBytes(dictionary, dictIdsBuffer[i]), valueBuffer);
        }
      } else {
        for (int i = 0; i < length; i++) {
          processList(i, extractFromString(dictionary, dictIdsBuffer[i]), valueBuffer);
        }
      }
    } else {
      switch (reader.getStoredType()) {
        case STRING:
          for (int i = 0; i < length; i++) {
            processList(i, extractFromString(reader, context, docIds[i]), valueBuffer);
          }
          break;
        case BYTES:
          for (int i = 0; i < length; i++) {
            processList(i, extractFromBytes(reader, context, docIds[i]), valueBuffer);
          }
          break;
        default:
          throw new IllegalStateException();
      }
    }
  }

  public <T extends ForwardIndexReaderContext> void evaluateBlock(int[] docIds, int length,
      ForwardIndexReader<T> reader, T context, Dictionary dictionary, int[] dictIdsBuffer, String[][] valueBuffer) {
    if (reader.isDictionaryEncoded()) {
      reader.readDictIds(docIds, length, dictIdsBuffer, context);
      if (dictionary.getValueType() == FieldSpec.DataType.BYTES) {
        for (int i = 0; i < length; i++) {
          processList(i, extractFromBytes(dictionary, dictIdsBuffer[i]), valueBuffer);
        }
      } else {
        for (int i = 0; i < length; i++) {
          processList(i, extractFromString(dictionary, dictIdsBuffer[i]), valueBuffer);
        }
      }
    } else {
      switch (reader.getStoredType()) {
        case STRING:
          for (int i = 0; i < length; i++) {
            processList(i, extractFromString(reader, context, docIds[i]), valueBuffer);
          }
          break;
        case BYTES:
          for (int i = 0; i < length; i++) {
            processList(i, extractFromBytes(reader, context, docIds[i]), valueBuffer);
          }
          break;
        default:
          throw new IllegalStateException();
      }
    }
  }

  @Override
  public <T extends ForwardIndexReaderContext> void evaluateBlock(int[] docIds, int length,
      ForwardIndexReader<T> reader, T context, Dictionary dictionary, int[] dictIdBuffer, Vector[] vector) {
    throw new UnsupportedOperationException();
  }

  @Nullable
  private <T> T extractFromBytes(Dictionary dictionary, int dictId) {
    try {
      return JSON_PARSER_CONTEXT.parseUtf8(dictionary.getBytesValue(dictId)).read(_jsonPath);
    } catch (Exception e) {
      return null;
    }
  }

  @Nullable
  private <T, R extends ForwardIndexReaderContext> T extractFromBytes(ForwardIndexReader<R> reader, R context,
      int docId) {
    try {
      return JSON_PARSER_CONTEXT.parseUtf8(reader.getBytes(docId, context)).read(_jsonPath);
    } catch (Exception e) {
      return null;
    }
  }

  @Nullable
  private <T> T extractFromBytesWithExactBigDecimal(Dictionary dictionary, int dictId) {
    try {
      return JSON_PARSER_CONTEXT_WITH_BIG_DECIMAL.parseUtf8(dictionary.getBytesValue(dictId)).read(_jsonPath);
    } catch (Exception e) {
      return null;
    }
  }

  @Nullable
  private <R extends ForwardIndexReaderContext> BigDecimal extractFromBytesWithExactBigDecimal(
      ForwardIndexReader<R> reader, R context, int docId) {
    try {
      return JSON_PARSER_CONTEXT_WITH_BIG_DECIMAL.parseUtf8(reader.getBytes(docId, context)).read(_jsonPath);
    } catch (Exception e) {
      return null;
    }
  }

  @Nullable
  private <T> T extractFromString(Dictionary dictionary, int dictId) {
    try {
      return JSON_PARSER_CONTEXT.parse(dictionary.getStringValue(dictId)).read(_jsonPath);
    } catch (Exception e) {
      return null;
    }
  }

  @Nullable
  private <T, R extends ForwardIndexReaderContext> T extractFromString(ForwardIndexReader<R> reader, R context,
      int docId) {
    try {
      return JSON_PARSER_CONTEXT.parseUtf8(reader.getBytes(docId, context)).read(_jsonPath);
    } catch (Exception e) {
      return null;
    }
  }

  @Nullable
  private <T> T extractFromStringWithExactBigDecimal(Dictionary dictionary, int dictId) {
    try {
      return JSON_PARSER_CONTEXT_WITH_BIG_DECIMAL.parse(dictionary.getStringValue(dictId)).read(_jsonPath);
    } catch (Exception e) {
      return null;
    }
  }

  @Nullable
  private <R extends ForwardIndexReaderContext> BigDecimal extractFromStringWithExactBigDecimal(
      ForwardIndexReader<R> reader, R context, int docId) {
    try {
      return JSON_PARSER_CONTEXT_WITH_BIG_DECIMAL.parseUtf8(reader.getBytes(docId, context)).read(_jsonPath);
    } catch (Exception e) {
      return null;
    }
  }

  private void processValue(int index, Object value, int defaultValue, int[] valueBuffer) {
    if (value instanceof Number) {
      valueBuffer[index] = ((Number) value).intValue();
    } else if (value == null) {
      if (_defaultValue != null) {
        valueBuffer[index] = defaultValue;
      } else {
        throwPathNotFoundException();
      }
    } else {
      valueBuffer[index] = Integer.parseInt(value.toString());
    }
  }

  private void processValue(int index, Object value, long defaultValue, long[] valueBuffer) {
    if (value instanceof Number) {
      valueBuffer[index] = ((Number) value).longValue();
    } else if (value == null) {
      if (_defaultValue != null) {
        valueBuffer[index] = defaultValue;
      } else {
        throwPathNotFoundException();
      }
    } else {
      // Handle scientific notation
      valueBuffer[index] = (long) Double.parseDouble(value.toString());
    }
  }

  private void processValue(int index, Object value, float defaultValue, float[] valueBuffer) {
    if (value instanceof Number) {
      valueBuffer[index] = ((Number) value).floatValue();
    } else if (value == null) {
      if (_defaultValue != null) {
        valueBuffer[index] = defaultValue;
      } else {
        throwPathNotFoundException();
      }
    } else {
      valueBuffer[index] = Float.parseFloat(value.toString());
    }
  }

  private void processValue(int index, Object value, double defaultValue, double[] valueBuffer) {
    if (value instanceof Number) {
      valueBuffer[index] = ((Number) value).doubleValue();
    } else if (value == null) {
      if (_defaultValue != null) {
        valueBuffer[index] = defaultValue;
      } else {
        throwPathNotFoundException();
      }
    } else {
      valueBuffer[index] = Double.parseDouble(value.toString());
    }
  }

  private void processValue(int index, Object value, BigDecimal defaultValue, BigDecimal[] valueBuffer) {
    if (value instanceof BigDecimal) {
      valueBuffer[index] = (BigDecimal) value;
    } else if (value == null) {
      if (_defaultValue != null) {
        valueBuffer[index] = defaultValue;
      } else {
        throwPathNotFoundException();
      }
    } else {
      valueBuffer[index] = new BigDecimal(value.toString());
    }
  }

  private void processValue(int index, Object value, String[] valueBuffer) {
    if (value instanceof String) {
      valueBuffer[index] = (String) value;
    } else if (value == null) {
      if (_defaultValue != null) {
        valueBuffer[index] = _defaultValue.toString();
      } else {
        throwPathNotFoundException();
      }
    } else {
      valueBuffer[index] = JsonUtils.objectToJsonNode(value).toString();
    }
  }

  private void processList(int index, List<Integer> value, int[][] valuesBuffer) {
    if (value == null) {
      valuesBuffer[index] = EMPTY_INTS;
    } else {
      int numValues = value.size();
      int[] values = new int[numValues];
      for (int j = 0; j < numValues; j++) {
        values[j] = value.get(j);
      }
      valuesBuffer[index] = values;
    }
  }

  private void processList(int index, List<Long> value, long[][] valuesBuffer) {
    if (value == null) {
      valuesBuffer[index] = EMPTY_LONGS;
    } else {
      int numValues = value.size();
      long[] values = new long[numValues];
      for (int j = 0; j < numValues; j++) {
        values[j] = value.get(j);
      }
      valuesBuffer[index] = values;
    }
  }

  private void processList(int index, List<Float> value, float[][] valuesBuffer) {
    if (value == null) {
      valuesBuffer[index] = EMPTY_FLOATS;
    } else {
      int numValues = value.size();
      float[] values = new float[numValues];
      for (int j = 0; j < numValues; j++) {
        values[j] = value.get(j);
      }
      valuesBuffer[index] = values;
    }
  }

  private void processList(int index, List<Double> value, double[][] valuesBuffer) {
    if (value == null) {
      valuesBuffer[index] = EMPTY_DOUBLES;
    } else {
      int numValues = value.size();
      double[] values = new double[numValues];
      for (int j = 0; j < numValues; j++) {
        values[j] = value.get(j);
      }
      valuesBuffer[index] = values;
    }
  }

  private void processList(int index, List<String> value, String[][] valuesBuffer) {
    if (value == null) {
      valuesBuffer[index] = EMPTY_STRINGS;
    } else {
      int numValues = value.size();
      String[] values = new String[numValues];
      for (int j = 0; j < numValues; j++) {
        values[j] = value.get(j);
      }
      valuesBuffer[index] = values;
    }
  }

  private void throwPathNotFoundException() {
    throw new IllegalArgumentException("Illegal Json Path: " + _jsonPath.getPath() + " does not match document");
  }
}
