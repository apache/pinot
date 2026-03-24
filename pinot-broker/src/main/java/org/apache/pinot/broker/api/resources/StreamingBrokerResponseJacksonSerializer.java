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
      writeResultTable(value, gen, provider);
      writeMetainfo(value, gen, _keysComparator);
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
  private static void writeResultTable(StreamingBrokerResponse value, JsonGenerator gen, SerializerProvider provider)
      throws IOException, InterruptedException {
    DataSchema dataSchema = value.getDataSchema();
    if (dataSchema == null) {
      return;
    }

    gen.writeFieldName("resultTable");
    gen.writeStartObject();

    // write the data schema
    gen.writeFieldName("dataSchema");
    provider.defaultSerializeValue(dataSchema, gen);

    try {
      writeRowsIfAny(value, gen, provider, dataSchema);
    } finally {
      gen.writeEndObject(); // end of resultTable
    }
  }

  /// Serializes the rows from the StreamingBrokerResponse.
  ///
  /// This method consumes the data blocks from the response.
  private static void writeRowsIfAny(StreamingBrokerResponse value, JsonGenerator gen, SerializerProvider provider,
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
      // OBJECT columns are type-unstable (runtime type can vary per row), so cache serializers by runtime class.
      @SuppressWarnings("unchecked")
      Map<Class<?>, JsonSerializer<Object>>[] objectTypeColumnSerializers = new Map[width];
      DataSchema.ColumnDataType[] columnTypes = dataSchema.getColumnDataTypes();
      for (int colIdx = 0; colIdx < columnTypes.length; colIdx++) {
        DataSchema.ColumnDataType columnType = columnTypes[colIdx];
        if (columnType == DataSchema.ColumnDataType.OBJECT) {
          // Lazily populated during row serialization as new runtime classes are encountered.
          objectTypeColumnSerializers[colIdx] = new HashMap<>();
        } else {
          serializers[colIdx] = provider.findTypedValueSerializer(columnType.getExternalClass(), false, null);
        }
      }
      value.consumeData(data -> writeDataBlockContent(
          data, gen, columnTypes, serializers, objectTypeColumnSerializers, width, provider));
    } finally {
      gen.writeEndArray();
    }
  }

  private static void writeDataBlockContent(StreamingBrokerResponse.Data dataBlock, JsonGenerator gen,
      DataSchema.ColumnDataType[] columnTypes, JsonSerializer<Object>[] serializers,
      Map<Class<?>, JsonSerializer<Object>>[] objectTypeColumnSerializers, int width, SerializerProvider provider) {
    while (dataBlock.next()) {
      // Make sure every row array is closed even if one value fails to serialize.
      boolean rowStarted = false;
      try {
        gen.writeStartArray();
        rowStarted = true;
        for (int i = 0; i < width; i++) {
          Object rawValue = dataBlock.get(i);
          if (rawValue == null) {
            gen.writeNull();
          } else {
            DataSchema.ColumnDataType dataType = columnTypes[i];
            Object external = dataType.toExternal(rawValue);
            JsonSerializer<Object> serializer = serializers[i];
            if (serializer == null) {
              // OBJECT fallback path: resolve serializer for this runtime class once, then reuse from cache.
              Map<Class<?>, JsonSerializer<Object>> runtimeSerializers = objectTypeColumnSerializers[i];
              Class<?> runtimeClass = external.getClass();
              serializer = runtimeSerializers.get(runtimeClass);
              if (serializer == null) {
                serializer = provider.findTypedValueSerializer(runtimeClass, false, null);
                runtimeSerializers.put(runtimeClass, serializer);
              }
            }
            serializer.serialize(external, gen, provider);
          }
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      } finally {
        if (rowStarted) {
          try {
            gen.writeEndArray();
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        }
      }
    }
  }

  private static void writeMetainfo(
      StreamingBrokerResponse response,
      JsonGenerator gen,
      Comparator<String> keysComparator
  ) throws InterruptedException {
    ObjectNode metainfo = response.getMetaInfo().asJson();

    ArrayList<String> fieldNames = new ArrayList<>(metainfo.size());
    metainfo.fieldNames().forEachRemaining(fieldNames::add);
    fieldNames.sort(keysComparator);

    try {
      for (String fieldName : fieldNames) {
        gen.writeObjectField(fieldName, metainfo.get(fieldName));
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
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
