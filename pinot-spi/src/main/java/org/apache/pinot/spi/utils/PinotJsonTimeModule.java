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
package org.apache.pinot.spi.utils;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdScalarDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdScalarSerializer;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;


public class PinotJsonTimeModule extends SimpleModule {

  public PinotJsonTimeModule() {
    this.addSerializer(LocalDateTime.class, new LocalDateTimeSerializer(LocalDateTime.class));
    this.addDeserializer(LocalDateTime.class, new LocalDateTimeDeserializer(LocalDateTime.class));

    this.addSerializer(OffsetDateTime.class, new OffsetDateTimeSerializer(OffsetDateTime.class));
    this.addDeserializer(OffsetDateTime.class, new OffsetDateTimeDeserializer(OffsetDateTime.class));
  }

  public static class LocalDateTimeSerializer extends StdScalarSerializer<LocalDateTime> {

    public LocalDateTimeSerializer(Class<LocalDateTime> t) {
      super(LocalDateTime.class);
    }

    @Override
    public void serialize(LocalDateTime ts, JsonGenerator gen, SerializerProvider provider)
        throws IOException {
      // always serialize JSON as number, since this isn't
      gen.writeNumber(ts.toInstant(ZoneOffset.UTC).toEpochMilli());
    }
  }

  public static class LocalDateTimeDeserializer extends StdScalarDeserializer<LocalDateTime> {

    protected LocalDateTimeDeserializer(Class<?> vc) {
      super(LocalDateTime.class);
    }

    @Override
    public LocalDateTime deserialize(JsonParser parser, DeserializationContext context)
        throws IOException, JsonProcessingException {
      if (parser.getCurrentToken().isNumeric()) {
        return TimestampUtils.toTimestamp(parser.getLongValue());
      } else {
        return TimestampUtils.toTimestamp(parser.getText());
      }
    }
  }

  public static class OffsetDateTimeSerializer extends StdScalarSerializer<OffsetDateTime> {

    public OffsetDateTimeSerializer(Class<OffsetDateTime> t) {
      super(OffsetDateTime.class);
    }

    @Override
    public void serialize(OffsetDateTime ts, JsonGenerator gen, SerializerProvider provider)
        throws IOException {
      // always serialize JSON as number
      gen.writeNumber(ts.toInstant().toEpochMilli());
    }
  }

  public static class OffsetDateTimeDeserializer extends StdScalarDeserializer<OffsetDateTime> {

    protected OffsetDateTimeDeserializer(Class<?> vc) {
      super(OffsetDateTime.class);
    }

    @Override
    public OffsetDateTime deserialize(JsonParser parser, DeserializationContext context)
        throws IOException, JsonProcessingException {
      if (parser.getCurrentToken().isNumeric()) {
        return TimestampUtils.toTimestampWithTimeZone(parser.getLongValue());
      } else {
        return TimestampUtils.toTimestampWithTimeZone(parser.getText());
      }
    }
  }
}
