package com.linkedin.thirdeye.rootcause;

import java.util.List;
import java.util.Map;


public interface Aggregator {
  List<Entity> aggregate(Map<String, PipelineResult> results);
}
