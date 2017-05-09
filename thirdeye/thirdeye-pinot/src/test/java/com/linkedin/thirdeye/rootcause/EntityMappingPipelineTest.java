package com.linkedin.thirdeye.rootcause;

import com.linkedin.thirdeye.datalayer.bao.EntityToEntityMappingManager;
import com.linkedin.thirdeye.datalayer.dto.EntityToEntityMappingDTO;
import com.linkedin.thirdeye.datalayer.pojo.EntityToEntityMappingBean;
import com.linkedin.thirdeye.rootcause.impl.DimensionEntity;
import com.linkedin.thirdeye.rootcause.impl.EntityMappingPipeline;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.Test;


public class EntityMappingPipelineTest {
  private static final String DIMENSION_TO_DIMENSION = "DIMENSION_TO_DIMENSION";
  private static final String METRIC_TO_SERVICE = "METRIC_TO_SERVICE";

  private static final PipelineContext CONTEXT_DEFAULT = makeContext(makeEntities(1.0, "country:us", "country:at", "other:value"));

  @Test
  public void testNoMappingDefault() {
    EntityToEntityMappingManager entityDAO = new MockEntityToEntityMappingManager(Collections.<EntityToEntityMappingDTO>emptyList());
    EntityMappingPipeline p = new EntityMappingPipeline("output", Collections.<String>emptySet(), entityDAO, DIMENSION_TO_DIMENSION, false, false);

    List<Entity> entities = runPipeline(p, CONTEXT_DEFAULT);

    Assert.assertTrue(entities.isEmpty());
  }

  @Test
  public void testNoMappingRewriter() {
    EntityToEntityMappingManager entityDAO = new MockEntityToEntityMappingManager(Collections.<EntityToEntityMappingDTO>emptyList());
    EntityMappingPipeline p = new EntityMappingPipeline("output", Collections.<String>emptySet(), entityDAO, DIMENSION_TO_DIMENSION, true, false);

    List<Entity> entities = runPipeline(p, CONTEXT_DEFAULT);

    Assert.assertEquals(entities.size(), 3);
    Assert.assertEquals(entities.get(0).getUrn(), "thirdeye:dimension:country:at");
    Assert.assertEquals(entities.get(0).getScore(), 1.0);
    Assert.assertEquals(entities.get(1).getUrn(), "thirdeye:dimension:country:us");
    Assert.assertEquals(entities.get(1).getScore(), 1.0);
    Assert.assertEquals(entities.get(2).getUrn(), "thirdeye:dimension:other:value");
    Assert.assertEquals(entities.get(2).getScore(), 1.0);
  }

  @Test
  public void testMappingDefault() {
    Collection<EntityToEntityMappingDTO> mappings = new ArrayList<>();
    mappings.add(makeMapping("thirdeye:dimension:country:", "thirdeye:dimension:countryCode:", 0.5, DIMENSION_TO_DIMENSION));
    mappings.add(makeMapping("thirdeye:dimension:country:us", "thirdeye:dimension:countryCode:us", 0.8, DIMENSION_TO_DIMENSION));
    EntityToEntityMappingManager entityDAO = new MockEntityToEntityMappingManager(mappings);

    EntityMappingPipeline p = new EntityMappingPipeline("output", Collections.<String>emptySet(), entityDAO, DIMENSION_TO_DIMENSION, false, false);

    List<Entity> entities = runPipeline(p, CONTEXT_DEFAULT);

    Assert.assertEquals(entities.size(), 1);
    Assert.assertEquals(entities.get(0).getUrn(), "thirdeye:dimension:countryCode:us");
    Assert.assertEquals(entities.get(0).getScore(), 0.8);
  }

  @Test
  public void testMappingDefaultPrefix() {
    Collection<EntityToEntityMappingDTO> mappings = new ArrayList<>();
    mappings.add(makeMapping("thirdeye:dimension:country:", "thirdeye:dimension:countryCode:", 0.5, DIMENSION_TO_DIMENSION));
    EntityToEntityMappingManager entityDAO = new MockEntityToEntityMappingManager(mappings);

    EntityMappingPipeline p = new EntityMappingPipeline("output", Collections.<String>emptySet(), entityDAO, DIMENSION_TO_DIMENSION, false, true);

    List<Entity> entities = runPipeline(p, CONTEXT_DEFAULT);

    Assert.assertEquals(entities.size(), 2);
    Assert.assertEquals(entities.get(0).getUrn(), "thirdeye:dimension:countryCode:at");
    Assert.assertEquals(entities.get(0).getScore(), 0.5);
    Assert.assertEquals(entities.get(1).getUrn(), "thirdeye:dimension:countryCode:us");
    Assert.assertEquals(entities.get(1).getScore(), 0.5);
  }

  @Test
  public void testMappingRewriter() {
    Collection<EntityToEntityMappingDTO> mappings = new ArrayList<>();
    mappings.add(makeMapping("thirdeye:dimension:country:us", "thirdeye:dimension:countryCode:us", 0.8, DIMENSION_TO_DIMENSION));
    EntityToEntityMappingManager entityDAO = new MockEntityToEntityMappingManager(mappings);

    EntityMappingPipeline p = new EntityMappingPipeline("output", Collections.<String>emptySet(), entityDAO, DIMENSION_TO_DIMENSION, true, false);

    List<Entity> entities = runPipeline(p, CONTEXT_DEFAULT);

    Assert.assertEquals(entities.size(), 3);
    Assert.assertEquals(entities.get(0).getUrn(), "thirdeye:dimension:country:at");
    Assert.assertEquals(entities.get(0).getScore(), 1.0);
    Assert.assertEquals(entities.get(1).getUrn(), "thirdeye:dimension:other:value");
    Assert.assertEquals(entities.get(1).getScore(), 1.0);
    Assert.assertEquals(entities.get(2).getUrn(), "thirdeye:dimension:countryCode:us");
    Assert.assertEquals(entities.get(2).getScore(), 0.8);
  }

  @Test
  public void testMappingRewriterPrefix() {
    Collection<EntityToEntityMappingDTO> mappings = new ArrayList<>();
    mappings.add(makeMapping("thirdeye:dimension:country:", "thirdeye:dimension:countryCode:", 0.5, DIMENSION_TO_DIMENSION));
    EntityToEntityMappingManager entityDAO = new MockEntityToEntityMappingManager(mappings);

    EntityMappingPipeline p = new EntityMappingPipeline("output", Collections.<String>emptySet(), entityDAO, DIMENSION_TO_DIMENSION, true, true);

    List<Entity> entities = runPipeline(p, CONTEXT_DEFAULT);

    Assert.assertEquals(entities.size(), 3);
    Assert.assertEquals(entities.get(0).getUrn(), "thirdeye:dimension:other:value");
    Assert.assertEquals(entities.get(0).getScore(), 1.0);
    Assert.assertEquals(entities.get(1).getUrn(), "thirdeye:dimension:countryCode:at");
    Assert.assertEquals(entities.get(1).getScore(), 0.5);
    Assert.assertEquals(entities.get(2).getUrn(), "thirdeye:dimension:countryCode:us");
    Assert.assertEquals(entities.get(2).getScore(), 0.5);
  }

  private static List<Entity> runPipeline(Pipeline pipeline, PipelineContext context) {
    List<Entity> entities = new ArrayList<>(pipeline.run(context).getEntities());
    Collections.sort(entities, new Comparator<Entity>() {
      @Override
      public int compare(Entity o1, Entity o2) {
        return o1.getUrn().compareTo(o2.getUrn());
      }
    });
    Collections.sort(entities, Entity.HIGHEST_SCORE_FIRST);
    return entities;
  }

  private static Entity[] makeEntities(double score, String... dimensions) {
    Entity[] entities = new Entity[dimensions.length];
    int i=0;
    for(String dim : dimensions) {
      String[] parts = dim.split(":");
      entities[i++] = DimensionEntity.fromDimension(score, parts[0], parts[1]);
    }
    return entities;
  }

  private static PipelineContext makeContext(Entity... entities) {
    Map<String, Set<Entity>> inputs = new HashMap<>();
    inputs.put("default", new HashSet<Entity>(Arrays.asList(entities)));
    return new PipelineContext(inputs);
  }

  private static EntityToEntityMappingDTO makeMapping(String from, String to, double score, String type) {
    EntityToEntityMappingDTO dto = new EntityToEntityMappingDTO();
    dto.setFromURN(from);
    dto.setToURN(to);
    dto.setScore(score);
    dto.setMappingType(type);
    return dto;
  }

  private static class MockEntityToEntityMappingManager extends AbstractMockManager<EntityToEntityMappingDTO> implements EntityToEntityMappingManager {
    private final Collection<EntityToEntityMappingDTO> entities;

    public MockEntityToEntityMappingManager(Collection<EntityToEntityMappingDTO> entities) {
      this.entities = entities;
    }

    @Override
    public List<EntityToEntityMappingDTO> findByFromURN(String fromURN) {
      throw new AssertionError("not implemented");
    }

    @Override
    public List<EntityToEntityMappingDTO> findByToURN(String toURN) {
      throw new AssertionError("not implemented");
    }

    @Override
    public EntityToEntityMappingDTO findByFromAndToURN(String fromURN, String toURN) {
      throw new AssertionError("not implemented");
    }

    @Override
    public List<EntityToEntityMappingDTO> findByMappingType(String mappingType) {
      List<EntityToEntityMappingDTO> entities = new ArrayList<>();
      for(EntityToEntityMappingDTO dto : this.entities) {
        if(dto.getMappingType().equals(mappingType))
          entities.add(dto);
      }
      return entities;
    }

    @Override
    public List<EntityToEntityMappingDTO> findByFromURNAndMappingType(String fromURN, String mappingType) {
      throw new AssertionError("not implemented");
    }

    @Override
    public List<EntityToEntityMappingDTO> findByToURNAndMappingType(String toURN, String mappingType) {
      throw new AssertionError("not implemented");
    }
  }
}
