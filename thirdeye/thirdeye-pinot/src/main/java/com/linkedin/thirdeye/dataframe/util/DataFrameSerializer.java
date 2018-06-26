package com.linkedin.thirdeye.dataframe.util;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.Series;
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
