package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.rootcause.Entity;
import com.linkedin.thirdeye.rootcause.Pipeline;
import com.linkedin.thirdeye.rootcause.PipelineContext;
import com.linkedin.thirdeye.rootcause.PipelineResult;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * TopKPipeline is a generic pipeline implementation for ordering, filtering, and truncating incoming
 * Entities. The pipeline first filters incoming Entities based on their {@code class} and then
 * orders them based on score from highest to lowest. It finally truncates the result to at most
 * {@code k} elements and emits the result.
 */
public class TopKPipeline extends Pipeline {
  public static final String PROP_K = "k";
  public static final String PROP_CLASS = "class";

  public static final String PROP_CLASS_DEFAULT = Entity.class.getName();

  private final int k;
  private final Class<? extends Entity> clazz;

  /**
   * Constructor for dependency injection
   *
   * @param outputName pipeline output name
   * @param inputNames input pipeline names
   * @param clazz (super) class to filter by
   * @param k maximum number of result elements
   */
  public TopKPipeline(String outputName, Set<String> inputNames, Class<? extends Entity> clazz, int k) {
    super(outputName, inputNames);
    this.k = k;
    this.clazz = clazz;
  }

  /**
   * Alternate constructor for RCAFrameworkLoader
   *
   * @param outputName pipeline output name
   * @param inputNames input pipeline names
   * @param properties configuration properties ({@code PROP_K}, {@code PROP_CLASS})
   */
  @SuppressWarnings("unchecked")
  public TopKPipeline(String outputName, Set<String> inputNames, Map<String, Object> properties) throws Exception {
    super(outputName, inputNames);

    if(!properties.containsKey(PROP_K))
      throw new IllegalArgumentException(String.format("Property '%s' required, but not found", PROP_K));
    this.k = Integer.parseInt(properties.get(PROP_K).toString());

    String classProp = PROP_CLASS_DEFAULT;
    if(properties.containsKey(PROP_CLASS))
      classProp = properties.get(PROP_CLASS).toString();
    this.clazz = (Class<? extends Entity>) Class.forName(classProp);
  }

  @Override
  public PipelineResult run(PipelineContext context) {
   return new PipelineResult(context, EntityUtils.topk(context.filter(this.clazz), this.k));
  }
}
