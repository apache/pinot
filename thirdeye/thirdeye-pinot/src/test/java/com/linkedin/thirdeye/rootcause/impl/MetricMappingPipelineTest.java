package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.EntityToEntityMappingDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.rootcause.Entity;
import com.linkedin.thirdeye.rootcause.MockDatasetConfigManager;
import com.linkedin.thirdeye.rootcause.MockEntityToEntityMappingManager;
import com.linkedin.thirdeye.rootcause.MockMetricConfigManager;
import com.linkedin.thirdeye.rootcause.PipelineContext;
import com.linkedin.thirdeye.rootcause.PipelineResult;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class MetricMappingPipelineTest {
  MetricMappingPipeline pipeline;

  @BeforeMethod
  void beforeMethod() {
    List<MetricConfigDTO> metrics = Arrays.asList(
        makeMetric(100, "a"), // native
        makeMetric(101, "x"), // related to metric
        makeMetric(102, "x"), // related to dataset
        makeMetric(103, "b"), // metric-related-dataset metric
        makeMetric(104, "y"), // metric-related-dataset related metric
        makeMetric(105, "c"), // dataset-related-dataset metric
        makeMetric(106, "z"), // dataset-related-dataset related metric
        makeMetric(107, "z")); // unrelated
    List<DatasetConfigDTO> datasets = Arrays.asList(
        makeDataset("a"), // native
        makeDataset("b"), // related to metric
        makeDataset("c"), // related to dataset
        makeDataset("d")); // unrelated
    List<EntityToEntityMappingDTO> mappings = Arrays.asList(
        makeMapping("thirdeye:metric:100", "thirdeye:metric:101"),
        makeMapping("thirdeye:metric:100", "thirdeye:dataset:b"),
        makeMapping("thirdeye:dataset:a", "thirdeye:metric:102"),
        makeMapping("thirdeye:dataset:a", "thirdeye:dataset:c"),
        makeMapping("thirdeye:dataset:b", "thirdeye:metric:104"),
        makeMapping("thirdeye:dataset:c", "thirdeye:metric:106"));

    pipeline = new MetricMappingPipeline("OUTPUT", Collections.singleton("INPUT"),
        new MockMetricConfigManager(metrics), new MockDatasetConfigManager(datasets), new MockEntityToEntityMappingManager(mappings));
  }

  @AfterMethod
  void afterMethod() {

  }

  @Test
  public void testExploreMetrics() {
    Set<Entity> input = Collections.singleton((Entity) MetricEntity.fromMetric(1.0, 100));
    PipelineContext context = new PipelineContext(Collections.singletonMap("INPUT", input));

    List<MetricEntity> result = getSorted(pipeline.run(context));

    assertEquals(result.get(0), "thirdeye:metric:100", 1.0);
    assertEquals(result.get(1), "thirdeye:metric:101", 0.9);
    assertEquals(result.get(2), "thirdeye:metric:102", 0.9);
    assertEquals(result.get(3), "thirdeye:metric:103", 0.9);
    assertEquals(result.get(4), "thirdeye:metric:104", 0.81);
    assertEquals(result.get(5), "thirdeye:metric:105", 0.9);
    assertEquals(result.get(6), "thirdeye:metric:106", 0.81);
  }

  private static MetricConfigDTO makeMetric(long id, String dataset) {
    MetricConfigDTO dto = new MetricConfigDTO();
    dto.setId(id);
    dto.setDataset(dataset);
    return dto;
  }

  private static DatasetConfigDTO makeDataset(String dataset) {
    DatasetConfigDTO dto = new DatasetConfigDTO();
    dto.setDataset(dataset);
    return dto;
  }

  private static EntityToEntityMappingDTO makeMapping(String from, String to) {
    EntityToEntityMappingDTO dto = new EntityToEntityMappingDTO();
    dto.setFromURN(from);
    dto.setToURN(to);
    dto.setScore(0.9);
    return dto;
  }

  private static List<MetricEntity> getSorted(PipelineResult result) {
    List<MetricEntity> metrics = new ArrayList<>();
    for (Entity e : result.getEntities()) {
      metrics.add((MetricEntity) e);
    }

    Collections.sort(metrics, new Comparator<MetricEntity>() {
      @Override
      public int compare(MetricEntity o1, MetricEntity o2) {
        int urn = o1.getUrn().compareTo(o2.getUrn());
        if (urn != 0)
          return urn;
        return Double.compare(o1.getScore(), o2.getScore());
      }
    });

    return metrics;
  }

  private static void assertEquals(MetricEntity entity, String urn, double score) {
    Assert.assertEquals(entity.getUrn(), urn);
    Assert.assertEquals(entity.getScore(), score);
  }
}
