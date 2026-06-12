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
import java.util.List;
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
    IOException failure = null;
    try {
      gen.writeStartObject();
      ResultTableWriteResult resultTableWriteResult = writeResultTable(value, gen, provider);
      writeMetainfo(resultTableWriteResult._metainfo, gen, _keysComparator, resultTableWriteResult._numRowsResultSet);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      String errorMessage = "Thread interrupted while serializing broker response";
      LOGGER.error(errorMessage, e);
      failure = new IOException(errorMessage, e);
    } catch (IOException e) {
      failure = e;
    }
    // Attempt to close the top-level object cleanly. If the generator is mid-array due to a propagating failure,
    // writeEndObject() would throw and replace the root cause; attach any such cleanup failure as suppressed instead.
    try {
      gen.writeEndObject();
    } catch (IOException closeEx) {
      if (failure != null) {
        failure.addSuppressed(closeEx);
      } else {
        throw closeEx;
      }
    }
    if (failure != null) {
      throw failure;
    }
  }

  /// Writes all the data from the StreamingBrokerResponse into the "resultTable" field.
  ///
  /// This method consumes the data blocks from the response using lazy-commit: the "resultTable" JSON field is
  /// opened only when the first row arrives. This avoids full buffering while still allowing resultTable to be
  /// suppressed when 0 rows are produced with exceptions (e.g. errorOnNumGroupsLimit=true).
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

    int width = dataSchema.size();
    DataSchema.ColumnDataType[] columnTypes = dataSchema.getColumnDataTypes();

    // The lazy-open callback writes the resultTable header (field name, schema, rows array start) on first row.
    // If consumeData produces 0 rows it is never called, so the field is never committed to the JSON stream.
    DataBlockContentWriter writer =
        new DataBlockContentWriter(gen, provider, columnTypes, width);
    writer.setBeforeFirstRow(() -> {
      try {
        gen.writeFieldName("resultTable");
        gen.writeStartObject();
        gen.writeFieldName("dataSchema");
        provider.defaultSerializeValue(dataSchema, gen);
        gen.writeFieldName("rows");
        gen.writeStartArray();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    });

    StreamingBrokerResponse.Metainfo metainfo;
    try {
      metainfo = value.consumeData(writer::writeDataBlockContent);
    } catch (UncheckedIOException e) {
      throw (IOException) e.getCause();
    }

    if (writer.getRowCount() == 0) {
      // The lazy-open callback was never invoked — resultTable was not started.
      if (!metainfo.getExceptions().isEmpty()) {
        return new ResultTableWriteResult(metainfo, -1); // suppress resultTable
      }
      // Empty result set with no exceptions: write a complete empty resultTable.
      gen.writeFieldName("resultTable");
      gen.writeStartObject();
      gen.writeFieldName("dataSchema");
      provider.defaultSerializeValue(dataSchema, gen);
      gen.writeFieldName("rows");
      gen.writeStartArray();
      gen.writeEndArray();
      gen.writeEndObject();
      return new ResultTableWriteResult(metainfo, 0);
    }

    // Close the JSON structures opened lazily before the first row.
    gen.writeEndArray(); // rows
    gen.writeEndObject(); // resultTable
    return new ResultTableWriteResult(metainfo, writer.getRowCount());
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

  /// Conversion strategy applied to a raw cell value before JSON serialization.
  private enum ConversionStrategy {
    /// Value is already the correct type; no conversion needed.
    NONE,
    /// Apply {@code ColumnDataType.toExternal()}, then {@code ColumnDataType.format()} to produce a JSON-safe string.
    /// Used when the raw value must first be converted to the external type (e.g. long → Timestamp) and then
    /// formatted (Timestamp → String) before serialization.
    TO_EXTERNAL_THEN_FORMAT,
    /// Apply {@code ColumnDataType.toExternal()} to convert to the external type.
    TO_EXTERNAL,
    /// Apply {@code ColumnDataType.format()} to convert to a JSON-safe string (TIMESTAMP, BYTES, BIG_DECIMAL, etc.).
    FORMAT
  }

  private static final class DataBlockContentWriter {
    private final JsonGenerator _gen;
    private final SerializerProvider _provider;
    private final DataSchema.ColumnDataType[] _columnTypes;
    private final int _width;
    private int _rowCount;
    private boolean _firstBlockWritten; // tracks whether the TTFB flush has been emitted
    private Runnable _beforeFirstRow; // called lazily before the first row is written; null after first invocation

    // Per-column monomorphic inline cache: for a given raw-value class, remember the strategy + serializer.
    // On runtime class change, the cache entry is evicted and recomputed once (no try/catch in steady state).
    private final Class<?>[] _cacheRawClass;
    @SuppressWarnings("unchecked")
    private final JsonSerializer<Object>[] _cacheSerializer;
    private final ConversionStrategy[] _cacheStrategy;

    private DataBlockContentWriter(JsonGenerator gen, SerializerProvider provider,
        DataSchema.ColumnDataType[] columnTypes, int width) {
      _gen = gen;
      _provider = provider;
      _columnTypes = columnTypes;
      _width = width;
      _cacheRawClass = new Class<?>[width];
      _cacheSerializer = new JsonSerializer[width];
      _cacheStrategy = new ConversionStrategy[width];
    }

    private void setBeforeFirstRow(Runnable beforeFirstRow) {
      _beforeFirstRow = beforeFirstRow;
    }

    private int getRowCount() {
      return _rowCount;
    }

    /// Resolves the conversion strategy for a given (column, rawClass) pair without try/catch.
    ///
    /// Called once per (column, rawClass) on a cache miss. Inspects whether the raw class is already the external
    /// class, whether formatting is needed (TIMESTAMP, BYTES, BIG_DECIMAL, array variants), or whether
    /// {@code toExternal} is applicable. The result is cached so future rows with the same raw class bypass this.
    private ConversionStrategy resolveStrategy(int colIdx, Class<?> rawClass) {
      DataSchema.ColumnDataType columnType = _columnTypes[colIdx];
      Class<?> externalClass = columnType.getExternalClass();
      // Check whether this type needs formatting to a string representation first (TIMESTAMP, BYTES, BIG_DECIMAL).
      // formatForJsonIfNeeded is a type-check per columnType, not per rawClass, but we only reach here when rawClass
      // matches the type that would be formatted, so we can decide FORMAT once for this (colIdx, rawClass) pair.
      switch (columnType) {
        case BIG_DECIMAL:
          if (BigDecimal.class.isAssignableFrom(rawClass)) {
            return ConversionStrategy.FORMAT;
          }
          break;
        case TIMESTAMP:
          if (Timestamp.class.isAssignableFrom(rawClass)) {
            return ConversionStrategy.FORMAT;
          }
          break;
        case BYTES:
          if (byte[].class.isAssignableFrom(rawClass)) {
            return ConversionStrategy.FORMAT;
          }
          break;
        case TIMESTAMP_ARRAY:
          if (Timestamp[].class.isAssignableFrom(rawClass)) {
            return ConversionStrategy.FORMAT;
          }
          break;
        case BYTES_ARRAY:
          if (byte[][].class.isAssignableFrom(rawClass)) {
            return ConversionStrategy.FORMAT;
          }
          break;
        default:
          break;
      }
      if (externalClass.isAssignableFrom(rawClass)) {
        return ConversionStrategy.NONE;
      }
      // Only select TO_EXTERNAL when toExternal is expected to succeed without exception for this rawClass.
      // We probe once here; if it fails we stay with NONE (use raw value + runtime serializer).
      return ConversionStrategy.TO_EXTERNAL;
    }

    private void writeDataBlockContent(StreamingBrokerResponse.Data dataBlock) {
      while (dataBlock.next()) {
        if (_beforeFirstRow != null) {
          Runnable callback = _beforeFirstRow;
          _beforeFirstRow = null;
          callback.run(); // throws UncheckedIOException on JSON write failure
        }
        // Make sure every row array is closed even if one value fails to serialize.
        boolean rowStarted = false;
        try {
          _gen.writeStartArray();
          rowStarted = true;
          for (int i = 0; i < _width; i++) {
            Object rawValue = dataBlock.get(i);
            if (rawValue == null) {
              _gen.writeNull();
              continue;
            }
            // Monomorphic inline cache: if the raw class matches the cached class, reuse strategy + serializer.
            Class<?> rawClass = rawValue.getClass();
            JsonSerializer<Object> serializer;
            ConversionStrategy strategy;
            if (_cacheRawClass[i] == rawClass) {
              strategy = _cacheStrategy[i];
              serializer = _cacheSerializer[i];
            } else {
              // Cache miss: resolve strategy once for this (column, rawClass) pair.
              strategy = resolveStrategy(i, rawClass);
              Class<?> postConversionClass;
              if (strategy == ConversionStrategy.FORMAT) {
                // After formatting, the value is always a String.
                postConversionClass = String.class;
              } else if (strategy == ConversionStrategy.TO_EXTERNAL) {
                // Probe toExternal once to learn the output class; if it throws, fall back to NONE.
                // Also detect whether the converted value needs a further format() call (e.g. long→Timestamp→String).
                Object probed;
                try {
                  probed = _columnTypes[i].toExternal(rawValue);
                } catch (RuntimeException e) {
                  // toExternal does not apply for this rawClass; use raw value directly.
                  strategy = ConversionStrategy.NONE;
                  probed = rawValue;
                }
                // Check if the converted value still needs format() (e.g. Timestamp produced from toExternal).
                Object formatted = formatForJsonIfNeeded(_columnTypes[i], probed);
                if (formatted != probed) {
                  // toExternal followed by format is needed; the final output is always a String.
                  strategy = ConversionStrategy.TO_EXTERNAL_THEN_FORMAT;
                  postConversionClass = String.class;
                } else {
                  postConversionClass = probed.getClass();
                }
              } else {
                postConversionClass = rawClass;
              }
              serializer = _provider.findTypedValueSerializer(postConversionClass, false, null);
              _cacheRawClass[i] = rawClass;
              _cacheStrategy[i] = strategy;
              _cacheSerializer[i] = serializer;
            }
            // Apply the cached strategy to produce the value to serialize.
            Object valueToSerialize;
            switch (strategy) {
              case FORMAT:
                valueToSerialize = _columnTypes[i].format(rawValue);
                break;
              case TO_EXTERNAL_THEN_FORMAT:
                valueToSerialize = _columnTypes[i].format(_columnTypes[i].toExternal(rawValue));
                break;
              case TO_EXTERNAL:
                valueToSerialize = _columnTypes[i].toExternal(rawValue);
                break;
              default:
                valueToSerialize = rawValue;
                break;
            }
            serializer.serialize(valueToSerialize, _gen, _provider);
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
      // Flush after the first data block so bytes reach the client immediately (TTFB) rather than waiting
      // for Jackson's internal 8 KB output buffer to fill.
      if (!_firstBlockWritten) {
        _firstBlockWritten = true;
        try {
          _gen.flush();
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }
    }

    /// Returns the formatted version of {@code value} for columns that require string representation in JSON
    /// (TIMESTAMP, BYTES, BIG_DECIMAL, and their array variants). Returns the value unchanged for all other types.
    /// Used during cache-miss resolution to detect whether a post-{@code toExternal} value still needs formatting.
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
  ) throws IOException {
    ObjectNode metainfoJson = metainfo.asJson();
    ensureBaselineStats(metainfoJson);
    if (numRowsResultSet >= 0
        && (!metainfoJson.has("numRowsResultSet") || metainfoJson.get("numRowsResultSet").asInt() <= 0)) {
      metainfoJson.put("numRowsResultSet", numRowsResultSet);
    }

    ArrayList<String> fieldNames = new ArrayList<>(metainfoJson.size());
    metainfoJson.fieldNames().forEachRemaining(fieldNames::add);
    fieldNames.sort(keysComparator);

    for (String fieldName : fieldNames) {
      gen.writeObjectField(fieldName, metainfoJson.get(fieldName));
    }
  }

  // Fields that clients always expect to be present for shape parity with the eager (non-streaming) response.
  private static final List<String> BASELINE_STAT_FIELDS = List.of(
      "totalDocs",
      "numServersQueried",
      "numServersResponded",
      "numSegmentsQueried",
      "numSegmentsProcessed",
      "numSegmentsMatched",
      "numDocsScanned",
      "timeUsedMs",
      "numEntriesScannedInFilter",
      "numEntriesScannedPostFilter"
  );

  private static void ensureBaselineStats(ObjectNode metainfoJson) {
    for (String field : BASELINE_STAT_FIELDS) {
      if (!metainfoJson.has(field)) {
        metainfoJson.put(field, 0);
      }
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
