package com.linkedin.thirdeye.rootcause;

import java.util.List;
import java.util.Map;


public class PipelineResult {
  final Map<Entity, Double> scores;
  final Map<Entity, Metadata> metadata;

  public PipelineResult(Map<Entity, Double> scores, Map<Entity, Metadata> metadata) {
    this.scores = scores;
    this.metadata = metadata;
  }

  public Map<Entity, Double> getScores() {
    return scores;
  }

  public Map<Entity, Metadata> getMetadata() {
    return metadata;
  }
}
