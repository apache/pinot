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

package org.apache.pinot.thirdeye.rootcause;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;


public class MockMetricConfigManager extends AbstractMockManager<MetricConfigDTO> implements MetricConfigManager {
  private final Collection<MetricConfigDTO> metrics;

  public MockMetricConfigManager(Collection<MetricConfigDTO> metrics) {
    this.metrics = metrics;
  }

  @Override
  public MetricConfigDTO findById(final Long id) {
    Collection<MetricConfigDTO> output = Collections2.filter(this.metrics, new Predicate<MetricConfigDTO>() {
      @Override
      public boolean apply(MetricConfigDTO dto) {
        return dto.getId().equals(id);
      }
    });

    if (output.isEmpty())
      return null;
    return output.iterator().next();
  }

  @Override
  public List<MetricConfigDTO> findAll() {
    return new ArrayList<>(this.metrics);
  }

  @Override
  public List<MetricConfigDTO> findByDataset(final String dataset) {
    return new ArrayList<>(Collections2.filter(this.metrics, new Predicate<MetricConfigDTO>() {
      @Override
      public boolean apply(MetricConfigDTO dto) {
        return dto.getDataset().equals(dataset);
      }
    }));
  }

  @Override
  public MetricConfigDTO findByMetricAndDataset(String metricName, String dataset) {
    throw new AssertionError("not implemented");
  }

  @Override
  public MetricConfigDTO findByAliasAndDataset(String alias, String dataset) {
    throw new AssertionError("not implemented");
  }

  @Override
  public List<MetricConfigDTO> findActiveByDataset(String dataset) {
    throw new AssertionError("not implemented");
  }

  @Override
  public List<MetricConfigDTO> findWhereNameOrAliasLikeAndActive(String name) {
    throw new AssertionError("not implemented");
  }

  @Override
  public List<MetricConfigDTO> findWhereAliasLikeAndActive(Set<String> aliasParts) {
    throw new AssertionError("not implemented");
  }

  @Override
  public List<MetricConfigDTO> findByMetricName(String metricName) {
    throw new AssertionError("not implemented");
  }
}
