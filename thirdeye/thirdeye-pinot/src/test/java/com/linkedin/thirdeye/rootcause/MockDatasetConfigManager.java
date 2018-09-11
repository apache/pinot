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

package com.linkedin.thirdeye.rootcause;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


public class MockDatasetConfigManager extends AbstractMockManager<DatasetConfigDTO> implements DatasetConfigManager {
  private final Collection<DatasetConfigDTO> datasets;

  public MockDatasetConfigManager(Collection<DatasetConfigDTO> datasets) {
    this.datasets = datasets;
  }

  @Override
  public DatasetConfigDTO findById(final Long id) {
    Collection<DatasetConfigDTO> output = Collections2.filter(this.datasets, new Predicate<DatasetConfigDTO>() {
      @Override
      public boolean apply(DatasetConfigDTO dto) {
        return dto.getId().equals(id);
      }
    });

    if (output.isEmpty())
      return null;
    return output.iterator().next();
  }

  @Override
  public int deleteRecordsOlderThanDays(int days) {
    return 0;
  }

  @Override
  public List<DatasetConfigDTO> findAll() {
    return new ArrayList<>(this.datasets);
  }

  @Override
  public DatasetConfigDTO findByDataset(final String dataset) {
    Collection<DatasetConfigDTO> output = new ArrayList<>(Collections2.filter(this.datasets, new Predicate<DatasetConfigDTO>() {
      @Override
      public boolean apply(DatasetConfigDTO dto) {
        return dto.getDataset().equals(dataset);
      }
    }));

    if (output.isEmpty())
      return null;
    return output.iterator().next();
  }

  @Override
  public List<DatasetConfigDTO> findActive() {
    throw new AssertionError("not implemented");
  }

  @Override
  public List<DatasetConfigDTO> findActiveRequiresCompletenessCheck() {
    throw new AssertionError("not implemented");
  }
}
