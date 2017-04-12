package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.rootcause.Aggregator;
import com.linkedin.thirdeye.rootcause.Entity;
import com.linkedin.thirdeye.rootcause.PipelineResult;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class LinearAggregator implements Aggregator {

  @Override
  public List<Entity> aggregate(Map<String, PipelineResult> results) {
    Map<Entity, Double> scores = new HashMap<>();
    for(PipelineResult r : results.values()) {
      for(Map.Entry<Entity, Double> s : r.getScores().entrySet()) {
        Entity e = s.getKey();
        if (!scores.containsKey(e)) {
          scores.put(e, 0.0d);
        }
        scores.put(e, scores.get(e) + s.getValue());
      }
    }

    List<Map.Entry<Entity, Double>> sorted = new ArrayList<>(scores.entrySet());
    Collections.sort(sorted, new Comparator<Map.Entry<Entity, Double>>() {
      @Override
      public int compare(Map.Entry<Entity, Double> o1, Map.Entry<Entity, Double> o2) {
        return -Double.compare(o1.getValue(), o2.getValue());
      }
    });

    List<Entity> entities = new ArrayList<>();
    for(Map.Entry<Entity, Double> e : sorted) {
      entities.add(e.getKey());
    }

    return entities;
  }
}
