package com.linkedin.thirdeye.rootcause;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
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
