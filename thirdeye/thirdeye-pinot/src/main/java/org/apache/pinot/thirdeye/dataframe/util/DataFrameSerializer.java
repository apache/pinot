/*
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

package org.apache.pinot.thirdeye.dataframe.util;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.dataframe.Series;
import java.io.IOException;


public class DataFrameSerializer extends StdSerializer<DataFrame> {
  public DataFrameSerializer() {
    this(null);
  }

  public DataFrameSerializer(Class<DataFrame> t) {
    super(t);
  }

  @Override
  public void serialize(DataFrame dataFrame, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
      throws IOException {

    jsonGenerator.writeStartObject();
    for (String seriesName : dataFrame.getSeriesNames()) {
      jsonGenerator.writeArrayFieldStart(seriesName);

      Series s = dataFrame.get(seriesName);
      switch (s.type()) {
        case BOOLEAN:
        case LONG:
          for (int i = 0; i < s.size(); i++) {
            if (s.isNull(i)) {
              jsonGenerator.writeNull();
            } else {
              jsonGenerator.writeNumber(s.getLong(i));
            }
          }
          break;

        case DOUBLE:
          for (int i = 0; i < s.size(); i++) {
            if (s.isNull(i)) {
              jsonGenerator.writeNull();
            } else {
              jsonGenerator.writeNumber(s.getDouble(i));
            }
          }
          break;

        case STRING:
        case OBJECT:
          for (int i = 0; i < s.size(); i++) {
            if (s.isNull(i)) {
              jsonGenerator.writeNull();
            } else {
              jsonGenerator.writeString(s.getString(i));
            }
          }
          break;
      }

      jsonGenerator.writeEndArray();
    }

    jsonGenerator.writeEndObject();
  }
}
