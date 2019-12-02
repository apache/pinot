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

package org.apache.pinot.thirdeye.datalayer.bao.jdbc;

import com.google.inject.Singleton;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.naming.AuthenticationException;
import javax.ws.rs.NotAuthorizedException;
import org.apache.pinot.thirdeye.auth.ThirdEyeAuthFilter;
import org.apache.pinot.thirdeye.auth.ThirdEyePrincipal;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datalayer.pojo.DetectionConfigBean;
import org.apache.pinot.thirdeye.datalayer.pojo.MergedAnomalyResultBean;
import java.util.Collections;


@Singleton
public class DetectionConfigManagerImpl extends AbstractManagerImpl<DetectionConfigDTO> implements DetectionConfigManager {
  public DetectionConfigManagerImpl() {
    super(DetectionConfigDTO.class, DetectionConfigBean.class);
  }

  @Override
  public int update(DetectionConfigDTO detectionConfigDTO) {
    if (detectionConfigDTO.getId() == null) {
      Long id = save(detectionConfigDTO);
      if (id > 0) {
        return 1;
      } else {
        return 0;
      }
    } else {
      DetectionConfigBean detectionConfigBean = convertDetectionConfigDTO2Bean(detectionConfigDTO);
      return genericPojoDao.update(detectionConfigBean);
    }
  }

  @Override
  public Long save(DetectionConfigDTO detectionConfigDTO) {
    if (detectionConfigDTO.getId() != null) {
      //TODO: throw exception and force the caller to call update instead
      update(detectionConfigDTO);
      return detectionConfigDTO.getId();
    }

    DetectionConfigBean detectionConfigBean = convertDetectionConfigDTO2Bean(detectionConfigDTO);
    Long id = genericPojoDao.put(detectionConfigBean);
    detectionConfigDTO.setId(id);
    return id;
  }

  DetectionConfigBean convertDetectionConfigDTO2Bean(DetectionConfigDTO detectionConfigDTO){
    detectionConfigDTO.setComponents(Collections.emptyMap());
    DetectionConfigBean bean = convertDTO2Bean(detectionConfigDTO, DetectionConfigBean.class);
    return bean;
  }

  @Override
  public List<DetectionConfigDTO> findAllActive() {
    List<DetectionConfigDTO> detectionConfigs = findAll();
    return detectionConfigs.stream().filter(DetectionConfigBean::isActive).collect(Collectors.toList());
  }
}
