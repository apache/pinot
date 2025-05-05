package org.apache.pinot.broker.api.resources;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Comparator;
import org.apache.pinot.common.response.StreamingBrokerResponse;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// A utility class that serializes [StreamingBrokerResponse] into JSON using Jackson.
public class StreamingBrokerResponseJacksonSerializer {
  private static final Logger LOGGER = LoggerFactory.getLogger(StreamingBrokerResponseJacksonSerializer.class);

  private StreamingBrokerResponseJacksonSerializer() {
  }

  /// Serializes the given [StreamingBrokerResponse] into JSON using the given [JsonGenerator].
  public static void serialize(StreamingBrokerResponse value, JsonGenerator gen, Comparator<String> keysComparator)
      throws IOException {
    try {
      writeResultTable(value, gen);

      writeMetainfo(value, gen, keysComparator);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      String errorMessage = "Thread interrupted while serializing broker response";
      LOGGER.error(errorMessage, e);
      throw new IOException(errorMessage, e);
    }
  }

  /// Writes all the data from the StreamingBrokerResponse into the "resultTable" field.
  ///
  /// This method consumes the data blocks from the response.
  private static void writeResultTable(StreamingBrokerResponse value, JsonGenerator gen)
      throws IOException, InterruptedException {
    DataSchema dataSchema = value.getDataSchema();
    if (dataSchema == null) {
      return;
    }

    gen.writeStartObject("resultTable");

    // write the data schema
    gen.writeFieldName("dataSchema");
    SerializerProvider provider = JsonUtils.getSerializerProvider();
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
      // prepare serializers for each column to avoid looking up each time
      @SuppressWarnings("unchecked")
      JsonSerializer<Object>[] serializers = new JsonSerializer[width];
      DataSchema.ColumnDataType[] columnTypes = dataSchema.getColumnDataTypes();
      for (int colIdx = 0; colIdx < columnTypes.length; colIdx++) {
        serializers[colIdx] = provider.findTypedValueSerializer(columnTypes[colIdx].getExternalClass(), false, null);
      }

      value.consumeData(data -> {
        writeDataBlockContent(data, gen, columnTypes, serializers, width, provider);
      });
    } finally {
      gen.writeEndArray();
    }
  }

  private static void writeDataBlockContent(StreamingBrokerResponse.Data dataBlock, JsonGenerator gen,
      DataSchema.ColumnDataType[] columnTypes, JsonSerializer<Object>[] serializers, int width,
      SerializerProvider provider) {
    try {
      while (dataBlock.next()) {
        // write the row as an array
        gen.writeStartArray();
        for (int i = 0; i < width; i++) {
          Object rawValue = dataBlock.get(i);
          if (rawValue == null) {
            gen.writeNull();
          } else {
            DataSchema.ColumnDataType dataType = columnTypes[i];
            Object external = dataType.toExternal(rawValue);
            serializers[i].serialize(external, gen, provider);
          }
        }
        gen.writeEndArray();
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
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
}
