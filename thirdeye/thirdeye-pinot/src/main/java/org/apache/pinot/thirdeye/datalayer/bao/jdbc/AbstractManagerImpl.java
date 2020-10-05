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

import com.google.inject.persist.Transactional;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pinot.thirdeye.datalayer.bao.AbstractManager;
import org.apache.pinot.thirdeye.datalayer.dao.GenericPojoDao;
import org.apache.pinot.thirdeye.datalayer.dto.AbstractDTO;
import org.apache.pinot.thirdeye.datalayer.pojo.AbstractBean;
import org.apache.pinot.thirdeye.datalayer.util.Predicate;
import org.joda.time.DateTime;
import org.modelmapper.ModelMapper;
import org.modelmapper.convention.MatchingStrategies;

public abstract class AbstractManagerImpl<E extends AbstractDTO> implements AbstractManager<E> {
  protected static final ModelMapper MODEL_MAPPER = new ModelMapper();

  static {
    MODEL_MAPPER.getConfiguration().setMatchingStrategy(MatchingStrategies.STRICT);
  }

  private final Class<? extends AbstractDTO> dtoClass;
  private final Class<? extends AbstractBean> beanClass;
  protected GenericPojoDao genericPojoDao;

  protected AbstractManagerImpl(Class<? extends AbstractDTO> dtoClass,
      Class<? extends AbstractBean> beanClass) {
    this.dtoClass = dtoClass;
    this.beanClass = beanClass;
  }

  public void setGenericPojoDao(GenericPojoDao genericPojoDao) {
    this.genericPojoDao = genericPojoDao;
  }

  @Override
  public Long save(E entity) {
    if (entity.getId() != null) {
      //TODO: throw exception and force the caller to call update instead
      update(entity);
      return entity.getId();
    }
    AbstractBean bean = convertDTO2Bean(entity, beanClass);
    Long id = genericPojoDao.put(bean);
    entity.setId(id);
    return id;
  }

  @Override
  public int update(E entity, Predicate predicate) {
    AbstractBean bean = convertDTO2Bean(entity, beanClass);
    return genericPojoDao.update(bean, predicate);
  }

  @Override
  public int update(E entity) {
    AbstractBean bean = convertDTO2Bean(entity, beanClass);
    return genericPojoDao.update(bean);
  }

  // Test is located at TestAlertConfigManager.testBatchUpdate()
  @Override
  public int update(List<E> entities) {
    ArrayList<AbstractBean> beans = new ArrayList<>();
    for (E entity : entities) {
      beans.add(convertDTO2Bean(entity, beanClass));
    }
    return genericPojoDao.update(beans);
  }

  public E findById(Long id) {
    AbstractBean abstractBean = genericPojoDao.get(id, beanClass);
    if (abstractBean != null) {
      AbstractDTO abstractDTO = MODEL_MAPPER.map(abstractBean, dtoClass);
      return (E) abstractDTO;
    } else {
      return null;
    }
  }

  @Override
  public List<E> findByIds(List<Long> ids) {
    List<? extends AbstractBean> abstractBeans = genericPojoDao.get(ids, beanClass);
    List<E> abstractDTOs = new ArrayList<>();
    if (CollectionUtils.isNotEmpty(abstractBeans)) {
      for (AbstractBean abstractBean : abstractBeans) {
        E abstractDTO = (E) MODEL_MAPPER.map(abstractBean, dtoClass);
        abstractDTOs.add(abstractDTO);
      }
    }
    return abstractDTOs;
  }

  @Override
  public int delete(E entity) {
    return genericPojoDao.delete(entity.getId(), beanClass);
  }

  // Test is located at TestAlertConfigManager.testBatchDeletion()
  @Override
  public int deleteById(Long id) {
    return genericPojoDao.delete(id, beanClass);
  }

  @Override
  public int deleteByIds(List<Long> ids) {
    return genericPojoDao.delete(ids, beanClass);
  }

  @Override
  public int deleteByPredicate(Predicate predicate) {
    return genericPojoDao.deleteByPredicate(predicate, beanClass);
  }

  @Override
  @Transactional
  public int deleteRecordsOlderThanDays(int days) {
    DateTime expireDate = new DateTime().minusDays(days);
    Timestamp expireTimestamp = new Timestamp(expireDate.getMillis());
    Predicate timestampPredicate = Predicate.LT("createTime", expireTimestamp);
    return deleteByPredicate(timestampPredicate);
  }

  @Override
  public List<E> findAll() {
    List<? extends AbstractBean> list = genericPojoDao.getAll(beanClass);
    List<E> result = new ArrayList<>();
    for (AbstractBean bean : list) {
      AbstractDTO dto = MODEL_MAPPER.map(bean, dtoClass);
      result.add((E) dto);
    }
    return result;
  }

  @Override
  public List<E> findByParams(Map<String, Object> filters) {
    List<? extends AbstractBean> list = genericPojoDao.get(filters, beanClass);
    return convertBeanListToDTOList(list);
  }

  @Override
  public List<E> findByPredicate(Predicate predicate) {
    List<? extends AbstractBean> list = genericPojoDao.get(predicate, beanClass);
    return convertBeanListToDTOList(list);
  }

  @Override
  public List<Long> findIdsByPredicate(Predicate predicate) {
    return genericPojoDao.getIdsByPredicate(predicate, beanClass);
  }

  @Override
  public List<E> list(long limit, long offset) {
    return convertBeanListToDTOList(genericPojoDao.list(beanClass, limit, offset));
  }

  @Override
  public List<E> findByPredicateJsonVal(Predicate predicate) {
    return convertBeanListToDTOList(genericPojoDao.getByPredicateJsonVal(predicate, beanClass));
  }

  @Override
  public long count() {
    return genericPojoDao.count(beanClass);
  }

  protected List<E> convertBeanListToDTOList(List<? extends AbstractBean> beans) {
    List<E> result = new ArrayList<>();
    for (AbstractBean bean : beans) {
      result.add((E) convertBean2DTO(bean, dtoClass));
    }
    return result;
  }

  protected <T extends AbstractDTO> T convertBean2DTO(AbstractBean entity, Class<T> dtoClass) {
    try {
      AbstractDTO dto = dtoClass.newInstance();
      MODEL_MAPPER.map(entity, dto);
      return (T) dto;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected <T extends AbstractBean> T convertDTO2Bean(AbstractDTO entity, Class<T> beanClass) {
    try {
      AbstractBean bean = beanClass.newInstance();
      MODEL_MAPPER.map(entity, bean);
      return (T) bean;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
