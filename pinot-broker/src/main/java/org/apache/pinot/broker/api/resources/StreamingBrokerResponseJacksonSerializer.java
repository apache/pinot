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
package org.apache.pinot.broker.api.resources;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleSerializers;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.response.StreamingBrokerResponse;
import org.apache.pinot.common.utils.DataSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// A utility class that serializes [StreamingBrokerResponse] into JSON using Jackson.
public class StreamingBrokerResponseJacksonSerializer extends StdSerializer<StreamingBrokerResponse> {
  private static final Logger LOGGER = LoggerFactory.getLogger(StreamingBrokerResponseJacksonSerializer.class);
  private final Comparator<String> _keysComparator;

  public StreamingBrokerResponseJacksonSerializer(Comparator<String> keysComparator) {
    super(StreamingBrokerResponse.class);
    _keysComparator = keysComparator;
  }

  @Override
  public void serialize(StreamingBrokerResponse value, JsonGenerator gen, SerializerProvider provider)
      throws IOException {
    try {
      gen.writeStartObject();
      ResultTableWriteResult resultTableWriteResult = writeResultTable(value, gen, provider);
      writeMetainfo(resultTableWriteResult._metainfo, gen, _keysComparator, resultTableWriteResult._numRowsResultSet);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      String errorMessage = "Thread interrupted while serializing broker response";
      LOGGER.error(errorMessage, e);
      throw new IOException(errorMessage, e);
    } finally {
      gen.writeEndObject();
    }
  }

  /// Writes all the data from the StreamingBrokerResponse into the "resultTable" field.
  ///
  /// This method consumes the data blocks from the response.
  private static ResultTableWriteResult writeResultTable(StreamingBrokerResponse value, JsonGenerator gen,
      SerializerProvider provider)
      throws IOException, InterruptedException {
    if (!value.shouldSerializeResultTable()) {
      return consumeResultTableWithoutWritingJson(value);
    }
    DataSchema dataSchema = value.getDataSchema();
    if (dataSchema == null) {
      return new ResultTableWriteResult(value.consumeData(data -> {
      }), -1);
    }

    gen.writeFieldName("resultTable");
    gen.writeStartObject();

    // write the data schema
    gen.writeFieldName("dataSchema");
    provider.defaultSerializeValue(dataSchema, gen);

    try {
      RowsWriteResult rowsWriteResult = writeRowsIfAny(value, gen, provider, dataSchema);
      return new ResultTableWriteResult(rowsWriteResult._metainfo, rowsWriteResult._rowCount);
    } finally {
      gen.writeEndObject(); // end of resultTable
    }
  }

  /// Consumes the full row stream without writing `resultTable` JSON (for `dropResults` on streaming responses).
  private static ResultTableWriteResult consumeResultTableWithoutWritingJson(StreamingBrokerResponse value)
      throws InterruptedException {
    DataSchema dataSchema = value.getDataSchema();
    if (dataSchema == null) {
      return new ResultTableWriteResult(value.consumeData(data -> {
      }), -1);
    }
    int[] rowCountHolder = new int[1];
    StreamingBrokerResponse.Metainfo metainfo = value.consumeData(dataBlock -> {
      while (dataBlock.next()) {
        rowCountHolder[0]++;
      }
    });
    return new ResultTableWriteResult(metainfo, rowCountHolder[0]);
  }

  /// Serializes the rows from the StreamingBrokerResponse.
  ///
  /// This method consumes the data blocks from the response.
  private static RowsWriteResult writeRowsIfAny(StreamingBrokerResponse value, JsonGenerator gen,
      SerializerProvider provider,
      DataSchema dataSchema
  ) throws IOException, InterruptedException {
    gen.writeFieldName("rows");
    // write all the rows as an array of arrays
    gen.writeStartArray();

    try {
      int width = dataSchema.size();
      // Fast path: cache one serializer per type-stable column (INT, LONG, STRING, arrays, etc.).
      @SuppressWarnings("unchecked")
      JsonSerializer<Object>[] serializers = new JsonSerializer[width];
      // Runtime type can vary per row (OBJECT columns, or rows already converted/formatted by eager paths), so cache
      // serializers by runtime class for fallback.
      @SuppressWarnings("unchecked")
      Map<Class<?>, JsonSerializer<Object>>[] runtimeColumnSerializers = new Map[width];
      DataSchema.ColumnDataType[] columnTypes = dataSchema.getColumnDataTypes();
      for (int colIdx = 0; colIdx < columnTypes.length; colIdx++) {
        DataSchema.ColumnDataType columnType = columnTypes[colIdx];
        if (columnType != DataSchema.ColumnDataType.OBJECT) {
          serializers[colIdx] = provider.findTypedValueSerializer(columnType.getExternalClass(), false, null);
        }
      }
      DataBlockContentWriter writer =
          new DataBlockContentWriter(gen, provider, columnTypes, serializers, runtimeColumnSerializers, width);
      StreamingBrokerResponse.Metainfo metainfo = value.consumeData(writer::writeDataBlockContent);
      return new RowsWriteResult(metainfo, writer.getRowCount());
    } finally {
      gen.writeEndArray();
    }
  }

  private static final class DataBlockContentWriter {
    private final JsonGenerator _gen;
    private final SerializerProvider _provider;
    private final DataSchema.ColumnDataType[] _columnTypes;
    private final JsonSerializer<Object>[] _serializers;
    private final Map<Class<?>, JsonSerializer<Object>>[] _runtimeColumnSerializers;
    private final int _width;
    private int _rowCount;

    private DataBlockContentWriter(JsonGenerator gen, SerializerProvider provider,
        DataSchema.ColumnDataType[] columnTypes, JsonSerializer<Object>[] serializers,
        Map<Class<?>, JsonSerializer<Object>>[] runtimeColumnSerializers, int width) {
      _gen = gen;
      _provider = provider;
      _columnTypes = columnTypes;
      _serializers = serializers;
      _runtimeColumnSerializers = runtimeColumnSerializers;
      _width = width;
    }

    private int getRowCount() {
      return _rowCount;
    }

    private void writeDataBlockContent(StreamingBrokerResponse.Data dataBlock) {
      while (dataBlock.next()) {
        // Make sure every row array is closed even if one value fails to serialize.
        boolean rowStarted = false;
        try {
          _gen.writeStartArray();
          rowStarted = true;
          for (int i = 0; i < _width; i++) {
            Object rawValue = dataBlock.get(i);
            if (rawValue == null) {
              _gen.writeNull();
            } else {
              Object valueToSerialize = rawValue;
              Class<?> externalClass = _columnTypes[i].getExternalClass();
              if (!externalClass.isInstance(rawValue)) {
                try {
                  valueToSerialize = _columnTypes[i].toExternal(rawValue);
                } catch (RuntimeException e) {
                  // Fall back to runtime serializer of the original value when conversion is not applicable.
                  valueToSerialize = rawValue;
                }
              }
              valueToSerialize = formatForJsonIfNeeded(_columnTypes[i], valueToSerialize);
              JsonSerializer<Object> serializer = _serializers[i];
              if (serializer != null && externalClass.isInstance(valueToSerialize)) {
                serializer.serialize(valueToSerialize, _gen, _provider);
                continue;
              }
              // Fallback path: resolve serializer for this runtime class once, then reuse from cache.
              Map<Class<?>, JsonSerializer<Object>> runtimeSerializers = _runtimeColumnSerializers[i];
              if (runtimeSerializers == null) {
                runtimeSerializers = new HashMap<>();
                _runtimeColumnSerializers[i] = runtimeSerializers;
              }
              Class<?> runtimeClass = valueToSerialize.getClass();
              serializer = runtimeSerializers.get(runtimeClass);
              if (serializer == null) {
                serializer = _provider.findTypedValueSerializer(runtimeClass, false, null);
                runtimeSerializers.put(runtimeClass, serializer);
              }
              serializer.serialize(valueToSerialize, _gen, _provider);
            }
          }
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        } finally {
          if (rowStarted) {
            _rowCount++;
            try {
              _gen.writeEndArray();
            } catch (IOException e) {
              throw new UncheckedIOException(e);
            }
          }
        }
      }
    }

    private static Object formatForJsonIfNeeded(DataSchema.ColumnDataType columnType, Object value) {
      if (value == null) {
        return null;
      }
      switch (columnType) {
        case BIG_DECIMAL:
          return value instanceof BigDecimal ? columnType.format(value) : value;
        case TIMESTAMP:
          return value instanceof Timestamp ? columnType.format(value) : value;
        case BYTES:
          return value instanceof byte[] ? columnType.format(value) : value;
        case TIMESTAMP_ARRAY:
          return value instanceof Timestamp[] ? columnType.format(value) : value;
        case BYTES_ARRAY:
          return value instanceof byte[][] ? columnType.format(value) : value;
        default:
          return value;
      }
    }
  }

  private static void writeMetainfo(
      StreamingBrokerResponse.Metainfo metainfo,
      JsonGenerator gen,
      Comparator<String> keysComparator,
      int numRowsResultSet
  ) throws InterruptedException {
    ObjectNode metainfoJson = metainfo.asJson();
    ensureBaselineStats(metainfoJson);
    if (numRowsResultSet >= 0
        && (!metainfoJson.has("numRowsResultSet") || metainfoJson.get("numRowsResultSet").asInt() <= 0)) {
      metainfoJson.put("numRowsResultSet", numRowsResultSet);
    }

    ArrayList<String> fieldNames = new ArrayList<>(metainfoJson.size());
    metainfoJson.fieldNames().forEachRemaining(fieldNames::add);
    fieldNames.sort(keysComparator);

    try {
      for (String fieldName : fieldNames) {
        gen.writeObjectField(fieldName, metainfoJson.get(fieldName));
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static void ensureBaselineStats(ObjectNode metainfoJson) {
    if (!metainfoJson.has("totalDocs")) {
      metainfoJson.put("totalDocs", 0);
    }
    if (!metainfoJson.has("numServersQueried")) {
      metainfoJson.put("numServersQueried", 0);
    }
    if (!metainfoJson.has("numServersResponded")) {
      metainfoJson.put("numServersResponded", 0);
    }
    if (!metainfoJson.has("numSegmentsQueried")) {
      metainfoJson.put("numSegmentsQueried", 0);
    }
    if (!metainfoJson.has("numSegmentsProcessed")) {
      metainfoJson.put("numSegmentsProcessed", 0);
    }
    if (!metainfoJson.has("numSegmentsMatched")) {
      metainfoJson.put("numSegmentsMatched", 0);
    }
    if (!metainfoJson.has("numDocsScanned")) {
      metainfoJson.put("numDocsScanned", 0);
    }
    if (!metainfoJson.has("timeUsedMs")) {
      metainfoJson.put("timeUsedMs", 0);
    }
    if (!metainfoJson.has("numEntriesScannedInFilter")) {
      metainfoJson.put("numEntriesScannedInFilter", 0);
    }
    if (!metainfoJson.has("numEntriesScannedPostFilter")) {
      metainfoJson.put("numEntriesScannedPostFilter", 0);
    }
  }

  private static final class ResultTableWriteResult {
    private final StreamingBrokerResponse.Metainfo _metainfo;
    private final int _numRowsResultSet;

    private ResultTableWriteResult(StreamingBrokerResponse.Metainfo metainfo, int numRowsResultSet) {
      _metainfo = metainfo;
      _numRowsResultSet = numRowsResultSet;
    }
  }

  private static final class RowsWriteResult {
    private final StreamingBrokerResponse.Metainfo _metainfo;
    private final int _rowCount;

    private RowsWriteResult(StreamingBrokerResponse.Metainfo metainfo, int rowCount) {
      _metainfo = metainfo;
      _rowCount = rowCount;
    }
  }

  public static void registerModule(ObjectMapper mapper, Comparator<String> keysComparator) {
    mapper.registerModule(new JacksonModule(keysComparator));
  }

  public static class JacksonModule extends Module {
    private final Comparator<String> _keysComparator;

    public JacksonModule(Comparator<String> keysComparator) {
      _keysComparator = keysComparator;
    }

    @Override
    public String getModuleName() {
      return "StreamingBrokerResponseJacksonModule";
    }

    @Override
    public Version version() {
      return new Version(1, 0, 0, null, null, null);
    }

    @Override
    public void setupModule(SetupContext context) {
      context.addSerializers(
          new SimpleSerializers(
              List.of(new StreamingBrokerResponseJacksonSerializer(_keysComparator))));
    }
  }
}
