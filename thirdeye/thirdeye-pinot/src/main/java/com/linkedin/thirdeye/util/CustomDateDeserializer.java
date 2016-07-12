package com.linkedin.thirdeye.util;

import java.io.IOException;

import org.joda.time.DateTime;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

public class CustomDateDeserializer extends JsonDeserializer<DateTime> {

  @Override
  public DateTime deserialize(JsonParser jsonparser, DeserializationContext ctxt)
      throws IOException, JsonProcessingException {
    String date = jsonparser.getText();
    return DateTime.parse(date);
  }

}
