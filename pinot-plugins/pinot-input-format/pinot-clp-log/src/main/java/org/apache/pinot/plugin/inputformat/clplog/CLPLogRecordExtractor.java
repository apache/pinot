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
import com.yscope.clp.compressorfrontend.BuiltInVariableHandlingRuleVersions;
import com.yscope.clp.compressorfrontend.EncodedMessage;
import com.yscope.clp.compressorfrontend.MessageEncoder;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
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


/**
 * A record extractor for log events. For configuration options, see {@link CLPLogRecordExtractorConfig}. This is an
 * experimental feature.
 * <p></p>
 * The goal of this record extractor is to allow us to encode the fields specified in
 * {@link CLPLogRecordExtractorConfig} using CLP. CLP is a compressor designed to encode unstructured log messages in a
 * way that makes them more compressible. It does this by decomposing a message into three fields:
 * <ul>
 *   <li>the message's static text, called a log type;</li>
 *   <li>repetitive variable values, called dictionary variables; and</li>
 *   <li>non-repetitive variable values (called encoded variables since we encode them specially if possible).</li>
 * </ul>
 * For instance, if the field "message" is encoded, then the extractor will output:
 * <ul>
 *   <li>message_logtype</li>
 *   <li>message_dictionaryVars</li>
 *   <li>message_encodedVars</li>
 * </ul>
 * All remaining fields are processed in the same way as they are in
 * {@link org.apache.pinot.plugin.inputformat.json.JSONRecordExtractor}. Specifically:
 * <ul>
 *   <li>If the caller passed a set of fields to {@code init}, then only those fields are extracted from each
 *   record and any remaining fields are dropped.</li>
 *   <li>Otherwise, all fields are extracted from each record.</li>
 * </ul>
 * This class' implementation is based on {@link org.apache.pinot.plugin.inputformat.json.JSONRecordExtractor}.
 */
public class CLPLogRecordExtractor extends BaseRecordExtractor<Map<String, Object>> {
  // The maximum number of variables that can be stored in a cell (row of a single column).
  private static final int MAX_VARIABLES_PER_CELL = ForwardIndexType.MAX_MULTI_VALUES_PER_ROW;
  private static final Logger LOGGER = LoggerFactory.getLogger(CLPLogRecordExtractor.class);

  private Set<String> _fields;
  // Used to indicate whether the caller wants us to extract all fields. See
  // org.apache.pinot.spi.data.readers.RecordExtractor.init for details.
  private boolean _extractAll = false;
  private CLPLogRecordExtractorConfig _config;

  private EncodedMessage _clpEncodedMessage;
  private MessageEncoder _clpMessageEncoder;
  private String _unencodableFieldErrorLogtype = null;
  private String[] _unencodableFieldErrorDictionaryVars = null;
  private Long[] _unencodableFieldErrorEncodedVars = null;

  private String _topicName;
  private ServerMetrics _serverMetrics;
  PinotMeter _realtimeClpTooManyEncodedVarsMeter = null;
  PinotMeter _realtimeClpUnencodableMeter = null;
  PinotMeter _realtimeClpEncodedNonStringsMeter = null;

  public void init(Set<String> fields, @Nullable RecordExtractorConfig recordExtractorConfig, String topicName,
      ServerMetrics serverMetrics) {
    init(fields, recordExtractorConfig);
    _topicName = topicName;
    _serverMetrics = serverMetrics;
  }

  @Override
  public void init(Set<String> fields, @Nullable RecordExtractorConfig recordExtractorConfig) {
    _config = (CLPLogRecordExtractorConfig) recordExtractorConfig;
    if (fields == null || fields.isEmpty()) {
      _extractAll = true;
      _fields = Collections.emptySet();
    } else {
      _fields = new HashSet<>(fields);
      // Remove the fields to be CLP-encoded to make it easier to work with them
      // in `extract`
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

    if (_extractAll) {
      for (Map.Entry<String, Object> recordEntry : from.entrySet()) {
        String recordKey = recordEntry.getKey();
        Object recordValue = recordEntry.getValue();
        if (clpEncodedFieldNames.contains(recordKey)) {
          encodeFieldWithClp(recordKey, recordValue, to);
        } else {
          if (null != recordValue) {
            recordValue = convert(recordValue);
          }
          to.putValue(recordKey, recordValue);
        }
      }
      return to;
    }

    // Handle un-encoded fields
    for (String fieldName : _fields) {
      Object value = from.get(fieldName);
      if (null != value) {
        value = convert(value);
      }
      to.putValue(fieldName, value);
    }

    // Handle encoded fields
    for (String fieldName : _config.getFieldsForClpEncoding()) {
      Object value = from.get(fieldName);
      encodeFieldWithClp(fieldName, value, to);
    }
    return to;
  }

  /**
   * In addition to the basic conversion, we also needs to retain the type info of Boolean input.
   */
  @Override
  protected Object convertSingleValue(Object value) {
    if (value instanceof Boolean) {
      return value;
    }
    return super.convertSingleValue(value);
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
