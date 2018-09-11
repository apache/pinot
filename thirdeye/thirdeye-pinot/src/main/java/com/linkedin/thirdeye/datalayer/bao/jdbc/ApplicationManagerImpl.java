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

import com.linkedin.thirdeye.datalayer.bao.ApplicationManager;
import com.linkedin.thirdeye.datalayer.dto.ApplicationDTO;
import com.linkedin.thirdeye.datalayer.pojo.ApplicationBean;
import com.linkedin.thirdeye.datalayer.util.Predicate;
import java.util.ArrayList;
import java.util.List;


public class ApplicationManagerImpl extends AbstractManagerImpl<ApplicationDTO>
    implements ApplicationManager {

  public ApplicationManagerImpl() {
    super(ApplicationDTO.class, ApplicationBean.class);
  }

  public List<ApplicationDTO> findByName(String name) {
    Predicate predicate = Predicate.EQ("application", name);
    List<ApplicationBean> list = genericPojoDao.get(predicate, ApplicationBean.class);
    List<ApplicationDTO> result = new ArrayList<>();
    for (ApplicationBean abstractBean : list) {
      ApplicationDTO dto = MODEL_MAPPER.map(abstractBean, ApplicationDTO.class);
      result.add(dto);
    }
    return result;
  }
}
