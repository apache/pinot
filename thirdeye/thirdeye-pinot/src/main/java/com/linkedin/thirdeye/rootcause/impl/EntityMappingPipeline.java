package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.datalayer.bao.EntityToEntityMappingManager;
import com.linkedin.thirdeye.datalayer.dto.EntityToEntityMappingDTO;
import com.linkedin.thirdeye.datalayer.pojo.EntityToEntityMappingBean;
import com.linkedin.thirdeye.rootcause.Entity;
import com.linkedin.thirdeye.rootcause.Pipeline;
import com.linkedin.thirdeye.rootcause.PipelineContext;
import com.linkedin.thirdeye.rootcause.PipelineResult;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The EntityMappingPipeline is a generic implementation for emitting Entities based on
 * association with incoming Entities. For example, it may be used to generate "similar" metrics
 * for each incoming MetricEntity based on a per-defined mapping of Entity URNs to other URNs.
 */
public class EntityMappingPipeline extends Pipeline {
  private static final Logger LOG = LoggerFactory.getLogger(EntityMappingPipeline.class);

  public static final String PROP_MAPPING_TYPE = "mappingType";
  public static final String PROP_IS_REWRITER = "isRewriter";
  public static final String PROP_MATCH_PREFIX = "matchPrefix";

  private final EntityToEntityMappingManager entityDAO;
  private final EntityToEntityMappingBean.MappingType mappingType;
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
  public EntityMappingPipeline(String outputName, Set<String> inputNames, EntityToEntityMappingManager entityDAO, EntityToEntityMappingBean.MappingType mappingType, boolean isRewriter, boolean matchPrefix) {
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

    String isRewriterProp = String.valueOf(false);
    if(properties.containsKey(PROP_IS_REWRITER))
      isRewriterProp = properties.get(PROP_IS_REWRITER);

    String matchPrefixProp = String.valueOf(false);
    if(properties.containsKey(PROP_MATCH_PREFIX))
      matchPrefixProp = properties.get(PROP_MATCH_PREFIX);

    this.entityDAO = DAORegistry.getInstance().getEntityToEntityMappingDAO();
    this.mappingType = EntityToEntityMappingBean.MappingType.valueOf(mappingTypeProp);
    this.isRewriter = Boolean.parseBoolean(isRewriterProp);
    this.matchPrefix = Boolean.parseBoolean(matchPrefixProp);
  }

  @Override
  public PipelineResult run(PipelineContext context) {
    Set<Entity> entities = context.filter(Entity.class);

    Map<String, EntityToEntityMappingDTO> mappings = toMap(this.entityDAO.findByMappingType(this.mappingType));

    Set<Entity> output = new HashSet<>();
    for(Entity entity : entities) {
      EntityToEntityMappingDTO mapping = find(entity, mappings);

      if(mapping != null) {
        try {
          double score = entity.getScore() * mapping.getScore();
          Entity newEntity = EntityUtils.parseURN(mapping.getToURN(), score);
          LOG.info("Mapping {} [{}] {} to {} [{}] {}", entity.getScore(), entity.getClass().getSimpleName(), entity.getUrn(),
              newEntity.getScore(), newEntity.getClass().getSimpleName(), newEntity.getUrn());
          output.add(newEntity);
        } catch (Exception ex) {
          LOG.warn("Exception while mapping entity '{}' to '{}'. Skipping.", entity.getUrn(), mapping.getToURN());
        }
      } else {
        // no mapping found
        if(this.isRewriter)
          output.add(entity);
      }
    }

    return new PipelineResult(context, output);
  }

  private EntityToEntityMappingDTO find(Entity entity, Map<String, EntityToEntityMappingDTO> mappings) {
    return this.matchPrefix ? findPrefix(entity, mappings) : findFull(entity, mappings);
  }

  private EntityToEntityMappingDTO findPrefix(Entity entity, Map<String, EntityToEntityMappingDTO> mappings) {
    for(Map.Entry<String, EntityToEntityMappingDTO> mapping : mappings.entrySet()) {
      if(entity.getUrn().startsWith(mapping.getKey()))
        return mapping.getValue();
    }
    return null;
  }

  private EntityToEntityMappingDTO findFull(Entity entity, Map<String, EntityToEntityMappingDTO> mappings) {
    return mappings.get(entity.getUrn());
  }

  private static Map<String, EntityToEntityMappingDTO> toMap(Iterable<EntityToEntityMappingDTO> mappings) {
    Map<String, EntityToEntityMappingDTO> m = new HashMap<>();
    for(EntityToEntityMappingDTO dto : mappings) {
      m.put(dto.getFromURN(), dto);
    }
    return m;
  }
}
