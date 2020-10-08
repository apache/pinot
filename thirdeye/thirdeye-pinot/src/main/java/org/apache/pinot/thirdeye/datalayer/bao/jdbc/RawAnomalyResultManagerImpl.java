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

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.pinot.thirdeye.datalayer.bao.RawAnomalyResultManager;
import org.apache.pinot.thirdeye.datalayer.dao.GenericPojoDao;
import org.apache.pinot.thirdeye.datalayer.dto.AnomalyFeedbackDTO;
import org.apache.pinot.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import org.apache.pinot.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import org.apache.pinot.thirdeye.datalayer.pojo.AnomalyFeedbackBean;
import org.apache.pinot.thirdeye.datalayer.pojo.AnomalyFunctionBean;
import org.apache.pinot.thirdeye.datalayer.pojo.RawAnomalyResultBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Singleton
public class RawAnomalyResultManagerImpl extends AbstractManagerImpl<RawAnomalyResultDTO> implements RawAnomalyResultManager {
  private static final Logger LOG = LoggerFactory.getLogger(RawAnomalyResultManagerImpl.class);

  @Inject
  public RawAnomalyResultManagerImpl(GenericPojoDao genericPojoDao) {
    super(RawAnomalyResultDTO.class, RawAnomalyResultBean.class, genericPojoDao);
  }

  public Long save(RawAnomalyResultDTO entity) {
    if (entity.getId() != null) {
      //TODO: throw exception and force the caller to call update instead
      update(entity);
      return entity.getId();
    }
    RawAnomalyResultBean bean =
        (RawAnomalyResultBean) convertDTO2Bean(entity, RawAnomalyResultBean.class);
    if (entity.getFeedback() != null) {
      if (entity.getFeedback().getId() == null) {
        AnomalyFeedbackBean feedbackBean =
            (AnomalyFeedbackBean) convertDTO2Bean(entity.getFeedback(), AnomalyFeedbackBean.class);
        Long feedbackId = genericPojoDao.put(feedbackBean);
        entity.getFeedback().setId(feedbackId);
      }
      bean.setAnomalyFeedbackId(entity.getFeedback().getId());
    }
    if (entity.getFunction() != null) {
      bean.setFunctionId(entity.getFunction().getId());
    }
    Long id = genericPojoDao.put(bean);
    entity.setId(id);
    return id;
  }

  public int update(RawAnomalyResultDTO entity) {
    RawAnomalyResultBean bean =
        (RawAnomalyResultBean) convertDTO2Bean(entity, RawAnomalyResultBean.class);
    if (entity.getFeedback() != null) {
      if (entity.getFeedback().getId() == null) {
        AnomalyFeedbackBean feedbackBean =
            (AnomalyFeedbackBean) convertDTO2Bean(entity.getFeedback(), AnomalyFeedbackBean.class);
        Long feedbackId = genericPojoDao.put(feedbackBean);
        entity.getFeedback().setId(feedbackId);
      }
      bean.setAnomalyFeedbackId(entity.getFeedback().getId());
    }
    if (entity.getFunction() != null) {
      bean.setFunctionId(entity.getFunction().getId());
    }
    return genericPojoDao.update(bean);
  }

  public RawAnomalyResultDTO findById(Long id) {
    RawAnomalyResultBean rawAnomalyResultBean = genericPojoDao.get(id, RawAnomalyResultBean.class);
    if (rawAnomalyResultBean != null) {
      RawAnomalyResultDTO rawAnomalyResultDTO;
      rawAnomalyResultDTO = createRawAnomalyDTOFromBean(rawAnomalyResultBean);
      return rawAnomalyResultDTO;
    } else {
      return null;
    }
  }

  private RawAnomalyResultDTO createRawAnomalyDTOFromBean(RawAnomalyResultBean rawAnomalyResultBean) {
    RawAnomalyResultDTO rawAnomalyResultDTO;
    rawAnomalyResultDTO = MODEL_MAPPER.map(rawAnomalyResultBean, RawAnomalyResultDTO.class);
    if (rawAnomalyResultBean.getFunctionId() != null) {
      AnomalyFunctionBean anomalyFunctionBean =
          genericPojoDao.get(rawAnomalyResultBean.getFunctionId(), AnomalyFunctionBean.class);
      if (anomalyFunctionBean == null) {
        LOG.error("this anomaly function bean should not be null");
      }
      AnomalyFunctionDTO anomalyFunctionDTO = MODEL_MAPPER.map(anomalyFunctionBean, AnomalyFunctionDTO.class);
      rawAnomalyResultDTO.setFunction(anomalyFunctionDTO);
    }
    if (rawAnomalyResultBean.getAnomalyFeedbackId() != null) {
      AnomalyFeedbackBean anomalyFeedbackBean =
          genericPojoDao.get(rawAnomalyResultBean.getAnomalyFeedbackId(), AnomalyFeedbackBean.class);
      AnomalyFeedbackDTO anomalyFeedbackDTO = MODEL_MAPPER.map(anomalyFeedbackBean, AnomalyFeedbackDTO.class);
      rawAnomalyResultDTO.setFeedback(anomalyFeedbackDTO);
    }
    return rawAnomalyResultDTO;
  }
}
