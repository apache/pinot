package com.linkedin.thirdeye.views;

import com.linkedin.thirdeye.heatmap.HeatMapCell;
import io.dropwizard.views.View;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HeatMapComponentView extends View
{
  private static final int CELLS_PER_ROW = 5;

  private final Map<String, List<HeatMapCell>> data;

  public HeatMapComponentView(Map<String, List<HeatMapCell>> data)
  {
    super("heat-map-component.ftl");
    this.data = data;
  }

  public Map<String, List<List<HeatMapCell>>> getHeatMaps() throws Exception
  {
    Map<String, List<List<HeatMapCell>>> heatMaps = new HashMap<String, List<List<HeatMapCell>>>();

    for (Map.Entry<String, List<HeatMapCell>> entry : data.entrySet())
    {
      // Generate rows
      List<List<HeatMapCell>> rows = new ArrayList<List<HeatMapCell>>();
      List<HeatMapCell> currentRow = new ArrayList<HeatMapCell>(CELLS_PER_ROW);
      for (HeatMapCell cell : entry.getValue())
      {
        currentRow.add(cell);
        if (currentRow.size() == CELLS_PER_ROW)
        {
          rows.add(currentRow);
          currentRow = new ArrayList<HeatMapCell>(CELLS_PER_ROW);
        }
      }
      if (!currentRow.isEmpty())
      {
        rows.add(currentRow);
      }

      // Record
      heatMaps.put(entry.getKey(), rows);
    }

    return heatMaps;
  }
}
