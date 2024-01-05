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

package org.apache.pinot.segment.spi.index;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.util.StdConverter;
import java.io.IOException;
import java.util.Objects;


/**
 * Class that holds the configurations for onheap dictionary. This is used by the index reader when loading the
 * dictionary on-heap.
 */
@JsonSerialize(converter = OnHeapDictionaryConfig.Serializator.class)
@JsonDeserialize(using = OnHeapDictionaryConfig.Deserializator.class)
public class OnHeapDictionaryConfig {
  public static final int DEFAULT_INTERNER_CAPACITY = 32_000_000;


  private boolean _enableInterning;
  private int _internerCapacity;

  public OnHeapDictionaryConfig(boolean enableInterning, int internerCapacity) {
    _enableInterning = enableInterning;
    _internerCapacity = internerCapacity;
  }

  public boolean enableInterning() {
    return _enableInterning;
  }

  public int getInternerCapacity() {
    return _internerCapacity;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    OnHeapDictionaryConfig that = (OnHeapDictionaryConfig) o;
    return _enableInterning == that._enableInterning && _internerCapacity == that._internerCapacity;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), _enableInterning, _internerCapacity);
  }

  @Override
  public String toString() {
    return "\"enableInterning\":" + _enableInterning + ", \"internerCapacity\":" + _internerCapacity;
  }

  public static class Serializator extends StdConverter<OnHeapDictionaryConfig, String> {
    @Override
    public String convert(OnHeapDictionaryConfig value) {
      return "{\"enableInterning\":" + value.enableInterning() + ", \"internerCapacity\":" + value.getInternerCapacity()
          + "}";
    }
  }

  public static class Deserializator extends StdDeserializer<OnHeapDictionaryConfig> {
    public Deserializator() {
      super(OnHeapDictionaryConfig.class);
    }

    @Override
    public OnHeapDictionaryConfig deserialize(JsonParser p, DeserializationContext ctxt)
        throws IOException, JsonProcessingException {
      JsonNode node = p.getCodec().readTree(p);
      JsonNode enableInterningNode = node.get("enableInterning");
      JsonNode internerCapacityNode = node.get("internerCapacity");

      if (enableInterningNode != null && internerCapacityNode != null) {
        boolean enableInterning = enableInterningNode.asBoolean();
        int internerCapacity = internerCapacityNode.asInt();
        return new OnHeapDictionaryConfig(enableInterning, internerCapacity);
      } else if (enableInterningNode != null) {
        boolean enableInterning = enableInterningNode.asBoolean();
        return new OnHeapDictionaryConfig(enableInterning, DEFAULT_INTERNER_CAPACITY);
      } else {
        // If interning is disabled, onHeapDictionaryConfig is not needed.
        return null;
      }
    }
  }
}
