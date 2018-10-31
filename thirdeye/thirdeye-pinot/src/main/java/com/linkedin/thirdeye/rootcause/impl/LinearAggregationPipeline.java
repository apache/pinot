/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.thirdeye.rootcause.impl;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.DoubleSeries;
import com.linkedin.thirdeye.dataframe.StringSeries;
import com.linkedin.thirdeye.rootcause.Entity;
import com.linkedin.thirdeye.rootcause.Pipeline;
import com.linkedin.thirdeye.rootcause.PipelineContext;
import com.linkedin.thirdeye.rootcause.PipelineResult;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Implementation of an aggregator that handles the same entity being returned from multiple
 * pipelines by summing the entity's weights. It optionally allows to truncate the input for
 * each input pipeline <i>separately</i> to its top {@code k} elements before aggregation.
 */
public class LinearAggregationPipeline extends Pipeline {
  private static Logger LOG = LoggerFactory.getLogger(LinearAggregationPipeline.class);

  private final static String PROP_K = "k";
  private final static String PROP_K_DEFAULT = "-1";

  private static final String URN = "urn";
  private static final String SCORE = "score";

  private final int k;

  /**
   * Constructor for dependency injection
   *
   * @param outputName pipeline output name
   * @param inputNames input pipeline names
   * @param k top k truncation before aggregation ({@code -1} for unbounded)
   */
  public LinearAggregationPipeline(String outputName, Set<String> inputNames, int k) {
    super(outputName, inputNames);

    this.k = k;
  }

  /**
   * Alternate constructor for use by RCAFrameworkLoader
   *
   * @param outputName pipeline output name
   * @param inputNames input pipeline names
   * @param properties configuration properties ({@code PROP_K})
   */
  public LinearAggregationPipeline(String outputName, Set<String> inputNames, Map<String, Object> properties) {
    super(outputName, inputNames);

    String kProp = PROP_K_DEFAULT;
    if(properties.containsKey(PROP_K))
      kProp = properties.get(PROP_K).toString();
    this.k = Integer.parseInt(kProp);
  }

  @Override
  public PipelineResult run(PipelineContext context) {
    StringSeries.Builder urnBuilder = StringSeries.builder();
    DoubleSeries.Builder scoreBuilder = DoubleSeries.builder();

    Multimap<String, Entity> urn2entity = ArrayListMultimap.create();
    for(Map.Entry<String, Set<Entity>> entry : context.getInputs().entrySet()) {
      DataFrame df = toDataFrame(entry.getValue());

      if(this.k >= 0) {
        LOG.info("Truncating '{}' to {} entities (from {})", entry.getKey(), this.k, df.size());
        df = df.sortedBy(SCORE).tail(this.k);
      }

      LOG.info("{}:\n{}", entry.getKey(), df.toString(50, URN, SCORE));
      urnBuilder.addSeries(df.get(URN));
      scoreBuilder.addSeries(df.getDoubles(SCORE));

      for(Entity e : entry.getValue()) {
        urn2entity.put(e.getUrn(), e);
      }
    }

    StringSeries urns = urnBuilder.build();
    DoubleSeries scores = scoreBuilder.build();

    DataFrame df = new DataFrame();
    df.addSeries(URN, urns);
    df.addSeries(SCORE, scores);

    DataFrame grp = df.groupByValue(URN).aggregate(SCORE, DoubleSeries.SUM);
    grp = grp.sortedBy(SCORE).reverse();

    return new PipelineResult(context, toEntities(grp, URN, SCORE, urn2entity));
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

  private static Set<Entity> toEntities(DataFrame df, String colUrn, String colScore, Multimap<String, Entity> urn2entity) {
    Set<Entity> entities = new HashSet<>();
    for(int i=0; i<df.size(); i++) {
      final String urn = df.getString(colUrn, i);
      entities.add(new Entity(urn, df.getDouble(colScore, i), new ArrayList<>(urn2entity.get(urn))));
    }
    return entities;
  }

}
