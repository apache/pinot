package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.DoubleSeries;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;


public class StringMappingParser {
  private static final String S_FROM = "from";
  private static final String S_TO = "to";
  private static final String S_WEIGHT = "weight";

  public static Collection<StringMapping> fromCsv(Reader in, double defaultWeight) throws IOException {
    Collection<StringMapping> mappings = new ArrayList<>();

    DataFrame df = DataFrame.fromCsv(in);

    if(!df.contains(S_WEIGHT))
      df.addSeries(S_WEIGHT, DoubleSeries.fillValues(df.size(), defaultWeight));

    df.addSeries(S_WEIGHT, df.getDoubles(S_WEIGHT).fillNull(defaultWeight));

    for(int i=0; i<df.size(); i++) {
      String from = df.getString(S_FROM, i);
      String to = df.getString(S_TO, i);
      double weight = df.getDouble(S_WEIGHT, i);
      mappings.add(new StringMapping(from, to, weight));
    }

    return mappings;
  }

  public static Collection<StringMapping> fromMap(Map<String, String> map, double defaultWeight) {
    Collection<StringMapping> mappings = new ArrayList<>();

    for(Map.Entry<String, String> e : map.entrySet()) {
      String[] parts = e.getValue().split(",");
      String to = parts[0];
      double weight = defaultWeight;
      if(parts.length >= 2)
        weight = Double.parseDouble(parts[1]);
      mappings.add(new StringMapping(e.getKey(), to, weight));
    }

    return mappings;
  }
}
