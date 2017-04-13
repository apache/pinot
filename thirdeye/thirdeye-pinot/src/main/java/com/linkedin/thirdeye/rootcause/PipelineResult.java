package com.linkedin.thirdeye.rootcause;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


public class PipelineResult {
  final Map<Entity, Double> scores;

  public PipelineResult(Map<? extends Entity, Double> scores) {
    this.scores = new HashMap<>(scores);
  }

  public Map<Entity, Double> getScores() {
    return Collections.unmodifiableMap(scores);
  }
}
