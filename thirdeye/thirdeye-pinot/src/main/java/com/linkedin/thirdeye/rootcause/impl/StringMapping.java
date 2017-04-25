package com.linkedin.thirdeye.rootcause.impl;

import java.util.HashMap;
import java.util.Map;


public class StringMapping {
  public static Map<String, StringMapping> toMap(Iterable<StringMapping> mappings) {
    HashMap<String, StringMapping> map = new HashMap<>();
    for(StringMapping m : mappings) {
      map.put(m.getFrom(), m);
    }
    return map;
  }

  final String from;
  final String to;
  final double score;

  public StringMapping(String from, String to, double score) {
    this.from = from;
    this.to = to;
    this.score = score;
  }

  public String getFrom() {
    return from;
  }

  public String getTo() {
    return to;
  }

  public double getScore() {
    return score;
  }
}
