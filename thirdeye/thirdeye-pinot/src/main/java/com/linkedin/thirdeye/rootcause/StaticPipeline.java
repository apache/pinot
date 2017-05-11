package com.linkedin.thirdeye.rootcause;

import com.linkedin.thirdeye.rootcause.impl.EntityUtils;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * StaticPipeline emits a fixed set of entities as a result, regardless of the input. It is
 * used to encapsulate constants (such as user input) during framework execution.
 */
public class StaticPipeline extends Pipeline {
  private static final String PROP_ENTITIES = "entities";
  private static final String PROP_SCORES = "scores";

  private final Set<Entity> entities;

  /**
   * Constructor for dependency injection
   *
   * @param outputName pipeline output name
   * @param inputNames input pipeline names
   * @param entities entities to emit as result
   */
  public StaticPipeline(String outputName, Set<String> inputNames, Set<Entity> entities) {
    super(outputName, inputNames);
    this.entities = entities;
  }

  /**
   * Alternate constructor for use by PipelineLoader
   *
   * @param outputName pipeline output name
   * @param inputNames input pipeline names
   * @param properties configuration properties ({@code PROP_ENTITIES}, {@code PROP_SCORES})
   */
  public StaticPipeline(String outputName, Set<String> inputNames, Map<String, String> properties) {
    super(outputName, inputNames);

    if(!properties.containsKey(PROP_ENTITIES))
      throw new IllegalArgumentException(String.format("Property '%s' required, but not found", PROP_ENTITIES));

    this.entities = new HashSet<>();
    String entitiesProp = properties.get(PROP_ENTITIES);
    String[] urns = entitiesProp.split(",");

    List<Double> scores = new ArrayList<>();
    if(properties.containsKey(PROP_SCORES)) {
      // per entity score
      String scoresProp = properties.get(PROP_SCORES);
      String[] scoreStrings = scoresProp.split(",");
      for(String s : scoreStrings) {
        scores.add(Double.parseDouble(s));
      }

    } else {
      // default score
      for(int i=0; i<urns.length; i++)
        scores.add(1.0);
    }

    if(scores.size() != urns.length)
      throw new IllegalArgumentException(String.format("Requires equal number of scores [=%d] and entities [=%d]", scores.size(), urns.length));

    for(int i=0; i<urns.length; i++) {
      this.entities.add(EntityUtils.parseURN(urns[i], scores.get(i)));
    }
  }

  @Override
  public PipelineResult run(PipelineContext context) {
    return new PipelineResult(context, this.entities);
  }

}
