package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.DoubleSeries;
import com.linkedin.thirdeye.dataframe.Series;
import com.linkedin.thirdeye.dataframe.StringSeries;
import com.linkedin.thirdeye.rootcause.Aggregator;
import com.linkedin.thirdeye.rootcause.Entity;
import com.linkedin.thirdeye.rootcause.PipelineResult;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class LinearAggregator implements Aggregator {
  private static final String URN = "urn";
  private static final String ZSCORE = "zscore";

  @Override
  public List<Entity> aggregate(Map<String, PipelineResult> results) {
    StringSeries.Builder urns = StringSeries.builder();
    DoubleSeries.Builder zscores = DoubleSeries.builder();

    Map<String, Entity> urn2entity = new HashMap<>();
    for(PipelineResult r : results.values()) {
      urn2entity.putAll(URNUtils.mapEntityURNs(r.getScores().keySet()));
      appendScores(urns, zscores, r);
    }

    DataFrame df = new DataFrame();
    df.addSeries(URN, urns.build());
    df.addSeries(ZSCORE, zscores.build());

    DataFrame grp = df.groupBy(URN).aggregate(ZSCORE, DoubleSeries.SUM);
    grp = grp.sortedBy(Series.GROUP_VALUE).reverse();

    List<Entity> entities = new ArrayList<>();
    for(String urn : grp.getStrings(Series.GROUP_KEY).values()) {
      entities.add(urn2entity.get(urn));
    }

    return entities;
  }

  private static void appendScores(StringSeries.Builder globalUrns, DoubleSeries.Builder globalZScores, PipelineResult r) {
    String[] urns = new String[r.getScores().size()];
    double[] scores = new double[r.getScores().size()];

    int i = 0;
    for(Map.Entry<Entity, Double> entry : r.getScores().entrySet()) {
      urns[i] = entry.getKey().getUrn();
      scores[i] = entry.getValue();
      i++;
    }

    // standardize
    StringSeries urn = StringSeries.buildFrom(urns);
    DoubleSeries score = DoubleSeries.buildFrom(scores);
    DoubleSeries zscore = score.map(new DoubleSeries.DoubleZScore(score.mean(), score.std()));

    globalUrns.addSeries(urn);
    globalZScores.addSeries(zscore);
  }
}
