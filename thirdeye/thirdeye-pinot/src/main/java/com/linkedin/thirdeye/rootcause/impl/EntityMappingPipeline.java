package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.datalayer.bao.EntityToEntityMappingManager;
import com.linkedin.thirdeye.datalayer.dto.EntityToEntityMappingDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.rootcause.Entity;
import com.linkedin.thirdeye.rootcause.Pipeline;
import com.linkedin.thirdeye.rootcause.PipelineContext;
import com.linkedin.thirdeye.rootcause.PipelineResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The EntityMappingPipeline is a generic implementation for emitting Entities based on
 * association with incoming Entities. For example, it may be used to generate "similar" metrics
 * for each incoming MetricEntity based on a per-defined mapping of Entity URNs to other URNs.
 *
 * <br/><b>NOTE:</b> An entity may map to multiple different entities
 */
public class EntityMappingPipeline extends Pipeline {
  private static final Logger LOG = LoggerFactory.getLogger(EntityMappingPipeline.class);

  public static final String PROP_MAPPING_TYPE = "mappingType";
  public static final String PROP_IS_REWRITER = "isRewriter";
  public static final String PROP_MATCH_PREFIX = "matchPrefix";

  public static final String PROP_IS_REWRITER_DEFAULT = "false";
  public static final String PROP_MATCH_PREFIX_DEFAULT = "false";

  private final EntityToEntityMappingManager entityDAO;
  private final String mappingType;
  private final boolean isRewriter;
  private final boolean matchPrefix;

  /**
   * Constructor for dependency injection
   *
   * @param outputName pipeline output name
   * @param inputNames input pipeline names
   * @param entityDAO entity mapping DAO
   * @param mappingType entity mapping type
   * @param isRewriter enable rewriter mode (pass-through for entities without mapping)
   * @param matchPrefix match on URN prefix rather than entire URN
   */
  public EntityMappingPipeline(String outputName, Set<String> inputNames, EntityToEntityMappingManager entityDAO, String mappingType, boolean isRewriter, boolean matchPrefix) {
    super(outputName, inputNames);
    this.entityDAO = entityDAO;
    this.mappingType = mappingType;
    this.isRewriter = isRewriter;
    this.matchPrefix = matchPrefix;
  }

  /**
   * Alternate constructor for use by PipelineLoader
   *
   * @param outputName pipeline output name
   * @param inputNames input pipeline names
   * @param properties configuration properties ({@code PROP_MAPPING_TYPE}, {@code PROP_IS_REWRITER=false}, {@code PROP_MATCH_PREFIX=false})
   */
  public EntityMappingPipeline(String outputName, Set<String> inputNames, Map<String, String> properties) throws IOException {
    super(outputName, inputNames);

    if(!properties.containsKey(PROP_MAPPING_TYPE))
      throw new IllegalArgumentException(String.format("Property '%s' required, but not found", PROP_MAPPING_TYPE));
    String mappingTypeProp = properties.get(PROP_MAPPING_TYPE);

    String isRewriterProp = String.valueOf(PROP_IS_REWRITER_DEFAULT);
    if(properties.containsKey(PROP_IS_REWRITER))
      isRewriterProp = properties.get(PROP_IS_REWRITER);

    String matchPrefixProp = String.valueOf(PROP_MATCH_PREFIX_DEFAULT);
    if(properties.containsKey(PROP_MATCH_PREFIX))
      matchPrefixProp = properties.get(PROP_MATCH_PREFIX);

    this.entityDAO = DAORegistry.getInstance().getEntityToEntityMappingDAO();
    this.mappingType = mappingTypeProp;
    this.isRewriter = Boolean.parseBoolean(isRewriterProp);
    this.matchPrefix = Boolean.parseBoolean(matchPrefixProp);
  }

  @Override
  public PipelineResult run(PipelineContext context) {
    Set<Entity> entities = context.filter(Entity.class);

    Map<String, Set<EntityToEntityMappingDTO>> mappings = toMap(this.entityDAO.findByMappingType(this.mappingType));

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

    return new PipelineResult(context, output);
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
      entities.add(EntityUtils.parseURN(toURN, entity.getScore() * match.getScore()));
    }

    return entities;
  }

  private Set<Entity> replaceFull(Entity entity, Map<String, Set<EntityToEntityMappingDTO>> mappings) {
    Set<EntityToEntityMappingDTO> matches = mappings.get(entity.getUrn());
    if(matches == null || matches.isEmpty())
      return handleNoMapping(entity);

    Set<Entity> entities = new HashSet<>();
    for(EntityToEntityMappingDTO match : matches) {
      entities.add(EntityUtils.parseURN(match.getToURN(), entity.getScore() * match.getScore()));
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
}
