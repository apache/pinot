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
package org.apache.pinot.plugin.inputformat.clplog;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.yscope.clp.compressorfrontend.BuiltInVariableHandlingRuleVersions;
import com.yscope.clp.compressorfrontend.EncodedMessage;
import com.yscope.clp.compressorfrontend.MessageEncoder;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.segment.local.segment.index.forward.ForwardIndexType;
import org.apache.pinot.spi.data.readers.BaseRecordExtractor;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordExtractorConfig;
import org.apache.pinot.spi.metrics.PinotMeter;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.sql.parsers.rewriter.ClpRewriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Record extractor for log events. Experimental. For configuration options see [CLPLogRecordExtractorConfig].
///
/// Each field configured for CLP encoding is split into three sibling fields — `_logtype`,
/// `_dictionaryVars`, `_encodedVars`. All other fields are extracted as plain JSON values.
public class CLPLogRecordExtractor extends BaseRecordExtractor<Map<String, Object>> {
  // The maximum number of variables that can be stored in a cell (row of a single column).
  private static final int MAX_VARIABLES_PER_CELL = ForwardIndexType.MAX_MULTI_VALUES_PER_ROW;
  private static final Logger LOGGER = LoggerFactory.getLogger(CLPLogRecordExtractor.class);

  private final ServerMetrics _serverMetrics = ServerMetrics.get();

  private CLPLogRecordExtractorConfig _config;
  private String _topicName;

  private EncodedMessage _clpEncodedMessage;
  private MessageEncoder _clpMessageEncoder;
  private String _unencodableFieldErrorLogtype;
  private String[] _unencodableFieldErrorDictionaryVars;
  private Long[] _unencodableFieldErrorEncodedVars;

  private PinotMeter _realtimeClpTooManyEncodedVarsMeter;
  private PinotMeter _realtimeClpUnencodableMeter;
  private PinotMeter _realtimeClpEncodedNonStringsMeter;

  public void init(@Nullable Set<String> fields, RecordExtractorConfig config, String topicName) {
    init(fields, config);
    _topicName = topicName;
  }

  @Override
  protected void initConfig(RecordExtractorConfig config) {
    Preconditions.checkArgument(config instanceof CLPLogRecordExtractorConfig,
        "CLPLogRecordExtractor requires a CLPLogRecordExtractorConfig");
    _config = (CLPLogRecordExtractorConfig) config;
    if (!_extractAll) {
      // Drop the CLP-encoded fields from `_fields` so the un-encoded loop in `extract` doesn't visit them —
      // they're handled separately. The base sets `_fields` to an immutable copy, so re-create as a
      // mutable `HashSet` first.
      _fields = new HashSet<>(_fields);
      _fields.removeAll(_config.getFieldsForClpEncoding());
    }

    _clpEncodedMessage = new EncodedMessage();
    _clpMessageEncoder = new MessageEncoder(BuiltInVariableHandlingRuleVersions.VariablesSchemaV2,
        BuiltInVariableHandlingRuleVersions.VariableEncodingMethodsV1);

    String unencodableFieldError = _config.getUnencodableFieldError();
    if (null != unencodableFieldError) {
      try {
        _clpMessageEncoder.encodeMessage(unencodableFieldError, _clpEncodedMessage);
        _unencodableFieldErrorLogtype = _clpEncodedMessage.getLogTypeAsString();
        _unencodableFieldErrorDictionaryVars = _clpEncodedMessage.getDictionaryVarsAsStrings();
        _unencodableFieldErrorEncodedVars = _clpEncodedMessage.getEncodedVarsAsBoxedLongs();
      } catch (IOException e) {
        LOGGER.error("Can't encode 'unencodableFieldError' with CLP. error: {}", e.getMessage());
      }
    }
  }

  @Override
  public GenericRow extract(Map<String, Object> from, GenericRow to) {
    Set<String> clpEncodedFieldNames = _config.getFieldsForClpEncoding();

    // Preserve topic name if configured, regardless of _extractAll
    if (_config.getTopicNameDestinationColumn() != null) {
      to.putValue(_config.getTopicNameDestinationColumn(), _topicName);
    }

    if (_extractAll) {
      for (Map.Entry<String, Object> entry : from.entrySet()) {
        String key = entry.getKey();
        Object value = entry.getValue();
        if (clpEncodedFieldNames.contains(key)) {
          encodeFieldWithClp(key, value, to);
        } else {
          to.putValue(key, value != null ? convert(value) : null);
        }
      }
      return to;
    }

    // Handle un-encoded fields
    for (String fieldName : _fields) {
      Object value = from.get(fieldName);
      to.putValue(fieldName, value != null ? convert(value) : null);
    }

    // Handle encoded fields
    for (String fieldName : _config.getFieldsForClpEncoding()) {
      Object value = from.get(fieldName);
      encodeFieldWithClp(fieldName, value, to);
    }
    return to;
  }

  /// Walks a non-null Jackson-parsed value and produces the contract shape: `BigDecimal` for `BigInteger`
  /// (oversized ints), `Object[]` for JSON arrays, `Map<String, Object>` for JSON objects, pass-through for
  /// the other Jackson scalar types.
  private static Object convert(Object value) {
    // BigInteger widens (Pinot has no BigInteger type)
    if (value instanceof BigInteger) {
      return new BigDecimal((BigInteger) value);
    }
    // List
    if (value instanceof List) {
      //noinspection unchecked
      return convertList((List<Object>) value);
    }
    // Map
    if (value instanceof Map) {
      //noinspection unchecked
      return convertMap((Map<String, Object>) value);
    }
    // Single value pass-through (Boolean / Integer / Long / Double / String)
    return value;
  }

  private static Object[] convertList(List<Object> list) {
    int n = list.size();
    Object[] result = new Object[n];
    for (int i = 0; i < n; i++) {
      Object v = list.get(i);
      result[i] = v != null ? convert(v) : null;
    }
    return result;
  }

  private static Map<String, Object> convertMap(Map<String, Object> map) {
    Map<String, Object> result = Maps.newHashMapWithExpectedSize(map.size());
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      Object v = entry.getValue();
      result.put(entry.getKey(), v != null ? convert(v) : null);
    }
    return result;
  }

  /**
   * Encodes a field with CLP
   * <p></p>
   * Given a field "x", this will output three fields: "x_logtype", "x_dictionaryVars", "x_encodedVars"
   * @param key Key of the field to encode
   * @param value Value of the field to encode
   * @param to The output row
   */
  private void encodeFieldWithClp(String key, Object value, GenericRow to) {
    String logtype = null;
    Object[] dictVars = null;
    Object[] encodedVars = null;
    if (null != value) {
      boolean fieldIsUnencodable = false;

      // Get value as string
      String valueAsString = null;
      if (value instanceof String) {
        valueAsString = (String) value;
      } else {
        try {
          valueAsString = JsonUtils.objectToString(value);
          _realtimeClpEncodedNonStringsMeter =
              _serverMetrics.addMeteredTableValue(_topicName, ServerMeter.REALTIME_CLP_ENCODED_NON_STRINGS, 1,
                  _realtimeClpEncodedNonStringsMeter);
        } catch (JsonProcessingException ex) {
          LOGGER.error("Can't convert value of type {} to String (to encode with CLP). name: '{}', value: '{}'",
              value.getClass().getSimpleName(), key, value);
          fieldIsUnencodable = true;
        }
      }

      // Encode value with CLP
      if (null != valueAsString) {
        try {
          _clpMessageEncoder.encodeMessage(valueAsString, _clpEncodedMessage);
          logtype = _clpEncodedMessage.getLogTypeAsString();
          encodedVars = _clpEncodedMessage.getEncodedVarsAsBoxedLongs();
          dictVars = _clpEncodedMessage.getDictionaryVarsAsStrings();

          if ((null != dictVars && dictVars.length > MAX_VARIABLES_PER_CELL) || (null != encodedVars
              && encodedVars.length > MAX_VARIABLES_PER_CELL)) {
            _realtimeClpTooManyEncodedVarsMeter =
                _serverMetrics.addMeteredTableValue(_topicName, ServerMeter.REALTIME_CLP_TOO_MANY_ENCODED_VARS, 1,
                    _realtimeClpTooManyEncodedVarsMeter);
            fieldIsUnencodable = true;
          }
        } catch (IOException e) {
          _realtimeClpUnencodableMeter =
              _serverMetrics.addMeteredTableValue(_topicName, ServerMeter.REALTIME_CLP_UNENCODABLE, 1,
                  _realtimeClpUnencodableMeter);
          fieldIsUnencodable = true;
        }
      }

      if (fieldIsUnencodable) {
        String unencodableFieldSuffix = _config.getUnencodableFieldSuffix();
        if (null != unencodableFieldSuffix) {
          String unencodableFieldKey = key + unencodableFieldSuffix;
          to.putValue(unencodableFieldKey, value);
        }

        if (null != _config.getUnencodableFieldError()) {
          logtype = _unencodableFieldErrorLogtype;
          dictVars = _unencodableFieldErrorDictionaryVars;
          encodedVars = _unencodableFieldErrorEncodedVars;
        } else {
          logtype = null;
          dictVars = null;
          encodedVars = null;
        }
      }
    }

    to.putValue(key + ClpRewriter.LOGTYPE_COLUMN_SUFFIX, logtype);
    to.putValue(key + ClpRewriter.DICTIONARY_VARS_COLUMN_SUFFIX, dictVars);
    to.putValue(key + ClpRewriter.ENCODED_VARS_COLUMN_SUFFIX, encodedVars);

    if (!_config.getRemoveProcessedFields()) {
      to.putValue(key, value);
    }
  }
}
