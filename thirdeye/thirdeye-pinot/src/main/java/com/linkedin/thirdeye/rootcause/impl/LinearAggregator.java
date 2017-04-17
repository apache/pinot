package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.DoubleSeries;
import com.linkedin.thirdeye.dataframe.Series;
import com.linkedin.thirdeye.dataframe.StringSeries;
import com.linkedin.thirdeye.rootcause.Aggregator;
import com.linkedin.thirdeye.rootcause.Entity;
import com.linkedin.thirdeye.rootcause.PipelineResult;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LinearAggregator implements Aggregator {
  private static Logger LOG = LoggerFactory.getLogger(LinearAggregator.class);

  private static final String URN = "urn";
  private static final String SCORE = "score";

  @Override
  public List<Entity> aggregate(Map<String, PipelineResult> results) {
    StringSeries.Builder urnBuilder = StringSeries.builder();
    DoubleSeries.Builder scoreBuilder = DoubleSeries.builder();

    for(PipelineResult r : results.values()) {
      DataFrame df = toDataFrame(r.getEntities());
      urnBuilder.addSeries(df.get(URN));

      DoubleSeries s = df.getDoubles(SCORE);
      scoreBuilder.addSeries(s.divide(s.sum()));
    }

    StringSeries urns = urnBuilder.build();
    DoubleSeries scores = scoreBuilder.build();

    DataFrame df = new DataFrame();
    df.addSeries(URN, urns);
    df.addSeries(SCORE, scores.divide(scores.sum()));

    DataFrame grp = df.groupBy(URN).aggregate(SCORE, DoubleSeries.SUM);
    grp = grp.sortedBy(SCORE).reverse();

    return toEntities(grp, URN, SCORE);
  }

  private static DataFrame toDataFrame(Collection<Entity> entities) {
    String[] urns = new String[entities.size()];
    double[] scores = new double[entities.size()];
    int i = 0;
    for(Entity e : entities) {
      urns[i] = e.getUrn();
      scores[i] = e.getScore();
      i++;
    }
    return new DataFrame().addSeries(URN, urns).addSeries(SCORE, scores);
  }

  private static List<Entity> toEntities(DataFrame df, String colUrn, String colScore) {
    List<Entity> entities = new ArrayList<>(df.size());
    for(int i=0; i<df.size(); i++) {
      entities.add(new Entity(df.getString(colUrn, i), df.getDouble(colScore, i)));
    }
    return entities;
  }

}
