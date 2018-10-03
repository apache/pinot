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

package com.linkedin.thirdeye.datalayer.bao.jdbc;

import com.google.inject.Singleton;
import com.linkedin.thirdeye.datalayer.bao.ClassificationConfigManager;
import com.linkedin.thirdeye.datalayer.dto.ClassificationConfigDTO;
import com.linkedin.thirdeye.datalayer.pojo.ClassificationConfigBean;
import com.linkedin.thirdeye.datalayer.util.Predicate;
import java.util.List;
import org.apache.commons.collections.CollectionUtils;

@Singleton
public class ClassificationConfigManagerImpl extends AbstractManagerImpl<ClassificationConfigDTO>
    implements ClassificationConfigManager {

  protected ClassificationConfigManagerImpl() {
    super(ClassificationConfigDTO.class, ClassificationConfigBean.class);
  }

  @Override
  public List<ClassificationConfigDTO> findActives() {
    Predicate predicate = Predicate.EQ("active", true);
    return findByPredicate(predicate);
  }

  @Override
  public ClassificationConfigDTO findByName(String name) {
    Predicate predicate = Predicate.EQ("name", name);
    List<ClassificationConfigDTO> results = findByPredicate(predicate);

    if (CollectionUtils.isNotEmpty(results)) {
      return results.get(0);
    } else {
      return null;
    }
  }
}
