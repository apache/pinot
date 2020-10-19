/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pinot.thirdeye.rootcause.impl;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.EntityToEntityMappingDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.rootcause.Entity;
import org.apache.pinot.thirdeye.rootcause.MockDatasetConfigManager;
import org.apache.pinot.thirdeye.rootcause.MockEntityToEntityMappingManager;
import org.apache.pinot.thirdeye.rootcause.MockMetricConfigManager;
import org.apache.pinot.thirdeye.rootcause.PipelineContext;
import org.apache.pinot.thirdeye.rootcause.PipelineResult;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class MetricMappingPipelineTest {
  private static final String TYPE_DEFAULT = "DEFAULT";
  private static final String TYPE_DIMENSION = "DIMENSION_TO_DIMENSION";

  private List<MetricConfigDTO> metrics;
  private List<DatasetConfigDTO> datasets;
  private List<EntityToEntityMappingDTO> mappings;

  @BeforeMethod
  void beforeMethod() {
    this.metrics = Arrays.asList(
        makeMetric(100, "a"), // native
        makeMetric(101, "x"), // related to metric
        makeMetric(102, "x"), // related to dataset
        makeMetric(103, "b"), // metric-related-dataset metric
        makeMetric(104, "y"), // metric-related-dataset related metric
        makeMetric(105, "c"), // dataset-related-dataset metric
        makeMetric(106, "z"), // dataset-related-dataset related metric
        makeMetric(107, "z")); // unrelated

    this.datasets = Arrays.asList(
        makeDataset("a", Arrays.asList("L", "M", "N", "Q")), // native
        makeDataset("b", Arrays.asList("R", "M")), // related to metric
        makeDataset("c", Arrays.asList("M", "N")), // related to dataset
        makeDataset("x", Arrays.asList("S")), // related to dataset
        makeDataset("y", Arrays.asList("M")), // related to dataset
        makeDataset("z", Arrays.asList("N"))); // related to dataset

    this.mappings = Arrays.asList(
        makeMapping("thirdeye:metric:100", "thirdeye:metric:101", TYPE_DEFAULT),
        makeMapping("thirdeye:metric:100", "thirdeye:dataset:b", TYPE_DEFAULT),
        makeMapping("thirdeye:dataset:a", "thirdeye:metric:102", TYPE_DEFAULT),
        makeMapping("thirdeye:dataset:a", "thirdeye:dataset:c", TYPE_DEFAULT),
        makeMapping("thirdeye:dataset:b", "thirdeye:metric:104", TYPE_DEFAULT),
        makeMapping("thirdeye:dataset:c", "thirdeye:metric:106", TYPE_DEFAULT),
        makeMapping("thirdeye:dimension:L:", "thirdeye:dimension:Q:", TYPE_DIMENSION),
        makeMapping("thirdeye:dimension:Q:", "thirdeye:dimension:R:", TYPE_DIMENSION), // transitive mapping Q to R
        makeMapping("thirdeye:dimension:Q:1", "thirdeye:dimension:S:one", TYPE_DIMENSION)); // transitive mapping with value

  }

  @Test
  public void testExploreMetrics() {
    MetricMappingPipeline pipeline = new MetricMappingPipeline("OUTPUT", Collections.singleton("INPUT"), false, Collections.<String>emptySet(),
        new MockMetricConfigManager(metrics), new MockDatasetConfigManager(datasets), new MockEntityToEntityMappingManager(mappings));

    Set<Entity> input = Collections.singleton((Entity) MetricEntity.fromMetric(1.0, 100));
    PipelineContext context = new PipelineContext(Collections.singletonMap("INPUT", input));

    List<MetricEntity> result = getSorted(pipeline.run(context));

    Assert.assertEquals(result.size(), 7);
    assertEquals(result.get(0), "thirdeye:metric:100", 1.0);
    assertEquals(result.get(1), "thirdeye:metric:101", 0.9);
    assertEquals(result.get(2), "thirdeye:metric:102", 0.9);
    assertEquals(result.get(3), "thirdeye:metric:103", 0.9);
    assertEquals(result.get(4), "thirdeye:metric:104", 0.81);
    assertEquals(result.get(5), "thirdeye:metric:105", 0.9);
    assertEquals(result.get(6), "thirdeye:metric:106", 0.81);
  }

  @Test
  public void testExploreMetricsWithFilters() {
    MetricMappingPipeline pipeline = new MetricMappingPipeline("OUTPUT", Collections.singleton("INPUT"), true, Collections.<String>emptySet(),
        new MockMetricConfigManager(metrics), new MockDatasetConfigManager(datasets), new MockEntityToEntityMappingManager(mappings));

    Multimap<String, String> filters = ArrayListMultimap.create();
    filters.put("invalid", "0");
    filters.put("L", "1");
    filters.put("L", "2");
    filters.put("M", "3");
    filters.put("N", "4");

    Set<Entity> input = Collections.singleton((Entity) MetricEntity.fromMetric(1.0, 100, filters));
    PipelineContext context = new PipelineContext(Collections.singletonMap("INPUT", input));

    List<MetricEntity> result = getSorted(pipeline.run(context));

    Assert.assertEquals(result.size(), 7);
    assertEquals(result.get(0), "thirdeye:metric:100:L%3D1:L%3D2:M%3D3:N%3D4:Q%3D1:Q%3D2", 1.0);
    assertEquals(result.get(1), "thirdeye:metric:101:S%3Done", 0.9);
    assertEquals(result.get(2), "thirdeye:metric:102:S%3Done", 0.9);
    assertEquals(result.get(3), "thirdeye:metric:103:M%3D3:R%3D1:R%3D2", 0.9);
    assertEquals(result.get(4), "thirdeye:metric:104:M%3D3", 0.81);
    assertEquals(result.get(5), "thirdeye:metric:105:M%3D3:N%3D4", 0.9);
    assertEquals(result.get(6), "thirdeye:metric:106:N%3D4", 0.81);
  }

  private static MetricConfigDTO makeMetric(long id, String dataset) {
    MetricConfigDTO dto = new MetricConfigDTO();
    dto.setId(id);
    dto.setDataset(dataset);
    return dto;
  }

  private static DatasetConfigDTO makeDataset(String dataset, List<String> dimensions) {
    DatasetConfigDTO dto = new DatasetConfigDTO();
    dto.setDataset(dataset);
    dto.setDimensions(dimensions);
    return dto;
  }

  private static EntityToEntityMappingDTO makeMapping(String from, String to, String type) {
    EntityToEntityMappingDTO dto = new EntityToEntityMappingDTO();
    dto.setFromURN(from);
    dto.setToURN(to);
    dto.setScore(0.9);
    dto.setMappingType(type);
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
