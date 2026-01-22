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
package org.apache.pinot.spi.data;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.json.JsonMapper;
import java.io.IOException;


/**
 * Custom deserializer for {@link Schema} that properly handles ComplexFieldSpec with MAP/LIST types.
 *
 * <p>This deserializer reads the JSON as a tree and re-parses it using a clean ObjectMapper,
 * ensuring that empty objects {} and arrays [] are properly handled as Java collections
 * rather than Scala collections (which can happen when jackson-module-scala is on the classpath).
 */
public class SchemaDeserializer extends StdDeserializer<Schema> {

  // Use a clean ObjectMapper without any auto-discovered modules (including jackson-module-scala).
  // JsonMapper.builder() creates a mapper without auto-registering modules from classpath.
  // We add a MixIn to override the @JsonDeserialize annotation on Schema to avoid infinite recursion.
  private static final ObjectMapper CLEAN_MAPPER = createCleanMapper();

  private static ObjectMapper createCleanMapper() {
    final ObjectMapper mapper = JsonMapper.builder().build();
    // Override the @JsonDeserialize annotation on Schema to use default Jackson deserialization
    mapper.addMixIn(Schema.class, SchemaDeserializerOverrideMixin.class);
    return mapper;
  }

  /**
   * MixIn class that overrides the @JsonDeserialize annotation on Schema.
   * An empty @JsonDeserialize means "use default Jackson deserialization".
   */
  @JsonDeserialize
  abstract static class SchemaDeserializerOverrideMixin {
  }

  public SchemaDeserializer() {
    super(Schema.class);
  }

  @Override
  public Schema deserialize(final JsonParser p, final DeserializationContext ctxt)
      throws IOException {
    // Read the JSON tree - this normalizes any Scala collections to JsonNode
    final JsonNode node = p.readValueAsTree();

    // Use CLEAN_MAPPER to deserialize directly from JsonNode - avoids Scala collections and infinite recursion
    return CLEAN_MAPPER.treeToValue(node, Schema.class);
  }
}
