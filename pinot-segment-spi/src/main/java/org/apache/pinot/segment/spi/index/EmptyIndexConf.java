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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import java.io.IOException;


/**
 * This class can be used by indexes that do not require actual configuration but instead use a boolean to indicate
 * that the index should be enabled or disabled.
 *
 * Given an index MyIndexType with id <em>testIndex</em> that uses this class as config object C, the configuration
 * could be specified in a {@link org.apache.pinot.spi.config.table.FieldConfig} like:
 *
 * <pre>
 *   {
 *     name: fieldName,
 *     indexes: {
 *       ...
 *       testIndex: true
 *       ...
 *     }
 *   }
 * </pre>
 */
// See EmptyIndexConfTest to see some examples
@JsonDeserialize(using = EmptyIndexConf.Deserializer.class)
@JsonSerialize(using = EmptyIndexConf.Serializer.class)
public class EmptyIndexConf {

  public static final EmptyIndexConf INSTANCE = new EmptyIndexConf();

  private EmptyIndexConf() {
  }

  public static EmptyIndexConf getInstance() {
    return INSTANCE;
  }

  public static class Deserializer extends StdDeserializer<EmptyIndexConf> {
    public Deserializer() {
      super(EmptyIndexConf.class);
    }

    @Override
    public EmptyIndexConf deserialize(JsonParser p, DeserializationContext ctxt)
        throws IOException {
      if (p.getBooleanValue()) {
        p.nextToken();
        return EmptyIndexConf.INSTANCE;
      } else {
        p.nextToken();
        return null;
      }
    }
  }

  public static class Serializer extends StdSerializer<EmptyIndexConf> {
    public Serializer() {
      super(EmptyIndexConf.class);
    }

    @Override
    public void serialize(EmptyIndexConf value, JsonGenerator gen, SerializerProvider provider)
        throws IOException {
      gen.writeBoolean(true);
    }
  }
}
