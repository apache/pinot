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
import java.util.List;
import org.apache.pinot.thirdeye.datalayer.bao.ConfigManager;
import org.apache.pinot.thirdeye.datalayer.dao.GenericPojoDao;
import org.apache.pinot.thirdeye.datalayer.dto.ConfigDTO;
import org.apache.pinot.thirdeye.datalayer.pojo.ConfigBean;
import org.apache.pinot.thirdeye.datalayer.util.Predicate;


@Singleton
public class ConfigManagerImpl extends AbstractManagerImpl<ConfigDTO> implements ConfigManager {

  @Inject
  public ConfigManagerImpl(GenericPojoDao genericPojoDao) {
    super(ConfigDTO.class, ConfigBean.class, genericPojoDao);
  }

  @Override
  public List<ConfigDTO> findByNamespace(String namespace) {
    return super.findByPredicate(Predicate.EQ("namespace", namespace));
  }

  @Override
  public ConfigDTO findByNamespaceName(String namespace, String name) {
    List<ConfigDTO> configs = super.findByPredicate(Predicate.AND(
        Predicate.EQ("namespace", namespace),
        Predicate.EQ("name", name)
    ));

    if (configs.isEmpty()) {
      return null;
    }

    return configs.get(0);
  }

  @Override
  public void deleteByNamespaceName(String namespace, String name) {
    try {
      super.delete(this.findByNamespaceName(namespace, name));
    } catch (Exception ignore) {
      // left blank
    }
  }
}
