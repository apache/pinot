package com.linkedin.thirdeye.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.joda.time.DateTime;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

public class CustomListDateDeserializer extends JsonDeserializer<List<DateTime>> {

  @Override
  public List<DateTime> deserialize(JsonParser jp, DeserializationContext ctxt)
      throws IOException, JsonProcessingException {
    List<DateTime> dateTimes = new ArrayList<>();
    String dateTimesString = jp.getText();
    dateTimesString = dateTimesString.substring(1, dateTimesString.length() - 1);
    String[] tokens = dateTimesString.split(",");
    for (String token : tokens) {
      dateTimes.add(DateTime.parse(token.trim()));
    }
    return dateTimes;
  }

}
