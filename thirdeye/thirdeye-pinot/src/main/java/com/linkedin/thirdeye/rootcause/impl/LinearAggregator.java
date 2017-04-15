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
  private static final String NORMALIZED = "normalized";

  @Override
  public List<Entity> aggregate(Map<String, PipelineResult> results) {
    StringSeries.Builder urns = StringSeries.builder();
    DoubleSeries.Builder normalized = DoubleSeries.builder();

    for(PipelineResult r : results.values()) {
      DataFrame df = toDataFrame(r.getEntities());
      urns.addSeries(df.get(URN));
      if(allEqual(df.get(SCORE))) {
        normalized.fillValues(df.size(), 1.0d);
      } else {
        normalized.addSeries(df.getDoubles(SCORE).normalize());
      }
    }

    DataFrame df = new DataFrame();
    df.addSeries(URN, urns.build());
    df.addSeries(NORMALIZED, normalized.build());

    DataFrame grp = df.groupBy(URN).aggregate(NORMALIZED, DoubleSeries.SUM);
    grp = grp.sortedBy(Series.GROUP_VALUE).reverse();

    return toEntities(grp, Series.GROUP_KEY, Series.GROUP_VALUE);
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

  private static boolean allEqual(Series score) {
    return score.unique().size() <= 1;
  }

  private static List<Entity> toEntities(DataFrame df, String colUrn, String colScore) {
    List<Entity> entities = new ArrayList<>(df.size());
    for(int i=0; i<df.size(); i++) {
      entities.add(new Entity(df.getString(colUrn, i), df.getDouble(colScore, i)));
    }
    return entities;
  }

}
