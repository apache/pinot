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
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Sample implementation of an aggregator that handles
 * the same entity being returned from multiple pipelines by summing the entity's weights.
 */
public class LinearAggregationPipeline extends Pipeline {
  private static Logger LOG = LoggerFactory.getLogger(LinearAggregationPipeline.class);

  private static final String URN = "urn";
  private static final String SCORE = "score";

  /**
   * Constructor for dependency injection
   *
   * @param outputName pipeline output name
   * @param inputNames input pipeline names
   */
  public LinearAggregationPipeline(String outputName, Set<String> inputNames) {
    super(outputName, inputNames);
  }

  /**
   * Alternate constructor for use by PipelineLoader
   *
   * @param outputName pipeline output name
   * @param inputNames input pipeline names
   * @param ignore configuration properties (none)
   */
  public LinearAggregationPipeline(String outputName, Set<String> inputNames, Map<String, String> ignore) {
    super(outputName, inputNames);
  }

  @Override
  public PipelineResult run(PipelineContext context) {
    StringSeries.Builder urnBuilder = StringSeries.builder();
    DoubleSeries.Builder scoreBuilder = DoubleSeries.builder();

    for(Set<Entity> res : context.getInputs().values()) {
      DataFrame df = toDataFrame(res);
      urnBuilder.addSeries(df.get(URN));
      scoreBuilder.addSeries(df.getDoubles(SCORE));
    }

    StringSeries urns = urnBuilder.build();
    DoubleSeries scores = scoreBuilder.build();

    DataFrame df = new DataFrame();
    df.addSeries(URN, urns);
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
