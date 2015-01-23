package com.linkedin.thirdeye.heatmap;

import java.util.List;
import java.util.Map;

public interface HeatMap
{
  List<HeatMapCell> generateHeatMap(Map<String, Number> baseline, Map<String, Number> current);
}
