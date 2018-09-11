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

import com.linkedin.thirdeye.datalayer.bao.EntityToEntityMappingManager;
import com.linkedin.thirdeye.datalayer.dto.EntityToEntityMappingDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.rootcause.Entity;
import com.linkedin.thirdeye.rootcause.MaxScoreSet;
import com.linkedin.thirdeye.rootcause.Pipeline;
import com.linkedin.thirdeye.rootcause.PipelineContext;
import com.linkedin.thirdeye.rootcause.PipelineResult;
import com.linkedin.thirdeye.rootcause.util.EntityUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO handle MetricEntity tail
/**
 * The EntityMappingPipeline is a generic implementation for emitting Entities based on
 * association with incoming Entities. For example, it may be used to generate "similar" metrics
 * for each incoming MetricEntity based on a per-defined mapping of Entity URNs to other URNs.
 *
 * <br/><b>NOTE:</b> An entity may map to multiple different entities
 */
public class EntityMappingPipeline extends Pipeline {
  private static final Logger LOG = LoggerFactory.getLogger(EntityMappingPipeline.class);

  public static final String PROP_DIRECTION_REGULAR = "REGULAR";
  public static final String PROP_DIRECTION_REVERSE = "REVERSE";
  public static final String PROP_DIRECTION_BOTH = "BOTH";

  public static final String PROP_INPUT_FILTERS = "inputFilters";
  public static final String PROP_OUTPUT_FILTERS = "outputFilters";
  public static final String PROP_IS_REWRITER = "isRewriter";
  public static final String PROP_MATCH_PREFIX = "matchPrefix";
  public static final String PROP_IS_COLLECTOR = "isCollector";
  public static final String PROP_DIRECTION = "direction";
  public static final String PROP_ITERATIONS = "iterations";

  public static final boolean PROP_IS_REWRITER_DEFAULT = false;
  public static final boolean PROP_MATCH_PREFIX_DEFAULT = false;
  public static final boolean PROP_IS_COLLECTOR_DEFAULT = false;
  public static final String PROP_DIRECTION_DEFAULT = PROP_DIRECTION_REGULAR;
  public static final int PROP_ITERATIONS_DEFAULT = 1;

  private final EntityToEntityMappingManager entityDAO;
  private final Set<String> inputFilters;
  private final Set<String> outputFilters;
  private final boolean isRewriter;
  private final boolean matchPrefix;
  private final boolean isCollector;
  private final String direction;
  private final int iterations;

  /**
   * Constructor for dependency injection
   *
   * @param outputName pipeline output name
   * @param inputNames input pipeline names
   * @param entityDAO entity mapping DAO
   * @param inputFilters input type filter
   * @param outputFilters output type filter
   * @param isRewriter enable rewriter mode (pass-through for entities without mapping)
   * @param matchPrefix match on URN prefix rather than entire URN
   * @param isCollector emit iterated entities in output (does not include input entities)
   * @param direction apply mappings given direction
   * @param iterations number of iterations of transitive hull expansion
   */
  public EntityMappingPipeline(String outputName, Set<String> inputNames, EntityToEntityMappingManager entityDAO, Set<String> inputFilters, Set<String> outputFilters, boolean isRewriter, boolean matchPrefix, boolean isCollector, String direction, int iterations) {
    super(outputName, inputNames);
    this.entityDAO = entityDAO;
    this.inputFilters = inputFilters;
    this.outputFilters = outputFilters;
    this.isRewriter = isRewriter;
    this.matchPrefix = matchPrefix;
    this.isCollector = isCollector;
    this.direction = direction;
    this.iterations = iterations;
  }

  /**
   * Alternate constructor for use by RCAFrameworkLoader
   *
   * @param outputName pipeline output name
   * @param inputNames input pipeline names
   * @param properties configuration properties ({@code PROP_MAPPING_TYPE}, {@code PROP_IS_REWRITER=false}, {@code PROP_MATCH_PREFIX=false}, {@code PROP_IS_COLLECTOR=false}, {@code PROP_DIRECTION=REGULAR}, {@code PROP_ITERATIONS=1})
   */
  public EntityMappingPipeline(String outputName, Set<String> inputNames, Map<String, Object> properties) throws IOException {
    super(outputName, inputNames);

    this.entityDAO = DAORegistry.getInstance().getEntityToEntityMappingDAO();
    this.isRewriter = MapUtils.getBoolean(properties, PROP_IS_REWRITER, PROP_IS_REWRITER_DEFAULT);
    this.matchPrefix = MapUtils.getBoolean(properties, PROP_MATCH_PREFIX, PROP_MATCH_PREFIX_DEFAULT);
    this.isCollector = MapUtils.getBoolean(properties, PROP_IS_COLLECTOR, PROP_IS_COLLECTOR_DEFAULT);
    this.direction = MapUtils.getString(properties, PROP_DIRECTION, PROP_DIRECTION_DEFAULT);
    this.iterations = MapUtils.getInteger(properties, PROP_ITERATIONS, PROP_ITERATIONS_DEFAULT);

    if (!Arrays.asList(PROP_DIRECTION_REGULAR, PROP_DIRECTION_REVERSE, PROP_DIRECTION_BOTH).contains(this.direction)) {
      throw new IllegalArgumentException(String.format("Unknown direction '%s'", this.direction));
    }

    if (properties.containsKey(PROP_INPUT_FILTERS)) {
      this.inputFilters = new HashSet<>((Collection<String>) properties.get(PROP_INPUT_FILTERS));
    } else {
      this.inputFilters = new HashSet<>();
    }

    if (properties.containsKey(PROP_OUTPUT_FILTERS)) {
      this.outputFilters = new HashSet<>((Collection<String>) properties.get(PROP_OUTPUT_FILTERS));
    } else {
      this.outputFilters = new HashSet<>();
    }
  }

  @Override
  public PipelineResult run(PipelineContext context) {
    Set<Entity> entities = context.filter(Entity.class);

    final Map<String, Set<EntityToEntityMappingDTO>> mappings = toMap(this.filterMappings(this.getMappings()));

    // perform entity urn mapping
    final int inputSize = entities.size();
    entities = mapEntities(entities, mappings);
    LOG.info("Mapped {} entities to {} entities in iteration {}", inputSize, entities.size(), 1);

    for(int i=1; i<this.iterations; i++) {
      Set<Entity> result = mapEntities(entities, mappings);

      final int originalSize = entities.size();
      if(this.isCollector) {
        entities.addAll(result);
      } else {
        entities = result;
      }

      LOG.info("Mapped {} entities to {} entities in iteration {}", originalSize, entities.size(), i + 1);
    }

    // consolidate entity scores, use max
    return new PipelineResult(context, new MaxScoreSet<>(entities));
  }

  private Set<Entity> mapEntities(Set<Entity> entities, Map<String, Set<EntityToEntityMappingDTO>> mappings) {
    Set<Entity> output = new HashSet<>();
    for(Entity entity : entities) {
      try {
        Set<Entity> newEntities = replace(entity, mappings);

        if(LOG.isDebugEnabled()) {
          for(Entity ne : newEntities) {
            LOG.debug("Mapping {} [{}] {} to {} [{}] {}",
                entity.getScore(), entity.getClass().getSimpleName(), entity.getUrn(),
                ne.getScore(), ne.getClass().getSimpleName(), ne.getUrn());
          }
        }

        output.addAll(newEntities);
      } catch (Exception ex) {
        LOG.warn("Exception while mapping entity '{}'. Skipping.", entity.getUrn(), ex);
      }
    }
    return output;
  }

  private Set<Entity> replace(Entity entity, Map<String, Set<EntityToEntityMappingDTO>> mappings) {
    return this.matchPrefix ? replacePrefix(entity, mappings) : replaceFull(entity, mappings);
  }

  private Set<Entity> replacePrefix(Entity entity, Map<String, Set<EntityToEntityMappingDTO>> mappings) {
    List<EntityToEntityMappingDTO> matches = findPrefix(entity, mappings);
    if(matches == null || matches.isEmpty())
      return handleNoMapping(entity);

    Set<Entity> entities = new HashSet<>();
    for(EntityToEntityMappingDTO match : matches) {
      String postfix = entity.getUrn().substring(match.getFromURN().length());
      String toURN = match.getToURN() + postfix;
      entities.add(EntityUtils.parseURN(toURN, entity.getScore() * match.getScore()).withRelated(Collections.singletonList(entity)));
    }

    return entities;
  }

  private Set<Entity> replaceFull(Entity entity, Map<String, Set<EntityToEntityMappingDTO>> mappings) {
    Set<EntityToEntityMappingDTO> matches = mappings.get(entity.getUrn());
    if(matches == null || matches.isEmpty())
      return handleNoMapping(entity);

    Set<Entity> entities = new HashSet<>();
    for(EntityToEntityMappingDTO match : matches) {
      entities.add(EntityUtils.parseURN(match.getToURN(), entity.getScore() * match.getScore()).withRelated(Collections.singletonList(entity)));
    }

    return entities;
  }

  private Set<Entity> handleNoMapping(Entity e) {
    if(this.isRewriter)
      return Collections.singleton(e);
    return Collections.emptySet();
  }

  private List<EntityToEntityMappingDTO> findPrefix(Entity entity, Map<String, Set<EntityToEntityMappingDTO>> mappings) {
    List<EntityToEntityMappingDTO> matches = new ArrayList<>();
    for(Map.Entry<String, Set<EntityToEntityMappingDTO>> mapping : mappings.entrySet()) {
      if(entity.getUrn().startsWith(mapping.getKey()))
        matches.addAll(mapping.getValue());
    }
    return matches;
  }

  private List<EntityToEntityMappingDTO> getMappings() {
    List<EntityToEntityMappingDTO> mappings = this.entityDAO.findAll();

    switch (this.direction) {
      case PROP_DIRECTION_REGULAR:
        return mappings;
      case PROP_DIRECTION_REVERSE:
        return reverseDirection(mappings);
      case PROP_DIRECTION_BOTH:
        mappings.addAll(reverseDirection(mappings));
        return mappings;
      default:
        throw new IllegalArgumentException(String.format("Unknown direction '%s'", this.direction));
    }
  }

  private List<EntityToEntityMappingDTO> filterMappings(List<EntityToEntityMappingDTO> mappings) {
    List<EntityToEntityMappingDTO> output = new ArrayList<>();
    for (EntityToEntityMappingDTO mapping : mappings) {
      if (passesInputFilters(mapping) && passesOutputFilters(mapping)) {
        output.add(mapping);
      }
    }
    return output;
  }

  /**
   * Applies input filters with {@code OR} semantics. If the set of filters in empty, passes by default.
   *
   * @param mapping entity mapping
   * @return {@code true} if a filter matches or the set of filters is empty, otherwise {@code false}
   */
  private boolean passesInputFilters(EntityToEntityMappingDTO mapping) {
    if (this.inputFilters.isEmpty()) {
      return true;
    }
    for (String filter : this.inputFilters) {
      if (mapping.getFromURN().startsWith(filter)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Applies output filters with {@code OR} semantics. If the set of filters in empty, passes by default.
   *
   * @param mapping entity mapping
   * @return {@code true} if a filter matches or the set of filters is empty, otherwise {@code false}
   */
  private boolean passesOutputFilters(EntityToEntityMappingDTO mapping) {
    if (this.outputFilters.isEmpty()) {
      return true;
    }
    for (String filter : this.outputFilters) {
      if (mapping.getToURN().startsWith(filter)) {
        return true;
      }
    }
    return false;
  }

  private static Map<String, Set<EntityToEntityMappingDTO>> toMap(Iterable<EntityToEntityMappingDTO> mappings) {
    Map<String, Set<EntityToEntityMappingDTO>> map = new HashMap<>();
    for(EntityToEntityMappingDTO dto : mappings) {
      String key = dto.getFromURN();
      if(!map.containsKey(key))
        map.put(key, new HashSet<EntityToEntityMappingDTO>());
      map.get(key).add(dto);
    }
    return map;
  }

  private static List<EntityToEntityMappingDTO> reverseDirection(Iterable<EntityToEntityMappingDTO> mappings) {
    List<EntityToEntityMappingDTO> output = new ArrayList<>();
    for (EntityToEntityMappingDTO m : mappings) {
      EntityToEntityMappingDTO n = new EntityToEntityMappingDTO();
      n.setFromURN(m.getToURN());
      n.setToURN(m.getFromURN());
      n.setScore(m.getScore());
      output.add(n);
    }
    return output;
  }
}
