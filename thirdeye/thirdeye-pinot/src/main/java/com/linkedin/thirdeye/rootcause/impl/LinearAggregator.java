package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.DoubleSeries;
import com.linkedin.thirdeye.dataframe.StringSeries;
import com.linkedin.thirdeye.rootcause.Entity;
import com.linkedin.thirdeye.rootcause.Pipeline;
import com.linkedin.thirdeye.rootcause.PipelineContext;
import com.linkedin.thirdeye.rootcause.PipelineResult;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Sample implementation of an aggregator that normalizes weights across pipelines and handles
 * the same entity being returned from multiple pipelines by summing the entity's weights. Each
 * pipeline is weighted equally.
 */
public class LinearAggregator extends Pipeline {
  private static Logger LOG = LoggerFactory.getLogger(LinearAggregator.class);

  private static final String URN = "urn";
  private static final String SCORE = "score";

  public LinearAggregator(String name, Set<String> inputs) {
    super(name, inputs);
  }

  @Override
  public PipelineResult run(PipelineContext context) {
    StringSeries.Builder urnBuilder = StringSeries.builder();
    DoubleSeries.Builder scoreBuilder = DoubleSeries.builder();

    for(Set<Entity> res : context.getInputs().values()) {
      DataFrame df = toDataFrame(res);
      urnBuilder.addSeries(df.get(URN));
      scoreBuilder.addSeries(df.getDoubles(SCORE));

//      DoubleSeries s = df.getDoubles(SCORE);
//      scoreBuilder.addSeries(s.divide(s.sum()));
    }

    StringSeries urns = urnBuilder.build();
    DoubleSeries scores = scoreBuilder.build();

    DataFrame df = new DataFrame();
    df.addSeries(URN, urns);
//    df.addSeries(SCORE, scores.divide(scores.sum()));
    df.addSeries(SCORE, scores);

    DataFrame grp = df.groupBy(URN).aggregate(SCORE, DoubleSeries.SUM);
    grp = grp.sortedBy(SCORE).reverse();

    return new PipelineResult(context, toEntities(grp, URN, SCORE));
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

  private static Set<Entity> toEntities(DataFrame df, String colUrn, String colScore) {
    Set<Entity> entities = new HashSet<>();
    for(int i=0; i<df.size(); i++) {
      entities.add(new Entity(df.getString(colUrn, i), df.getDouble(colScore, i)));
    }
    return entities;
  }

}
