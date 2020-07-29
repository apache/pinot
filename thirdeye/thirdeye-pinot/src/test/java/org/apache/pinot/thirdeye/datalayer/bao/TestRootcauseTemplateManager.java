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

package org.apache.pinot.thirdeye.datalayer.bao;

import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.thirdeye.datalayer.dto.RootcauseTemplateDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestRootcauseTemplateManager {
  private DAOTestBase testDAOProvider;
  private RootcauseTemplateManager templateDao;
  private final static String TEMPLATE_NAME = "test_template_";
  private final static String APPLICATION_NAME = "test-app";

  @BeforeMethod
  void beforeMethod() {
    testDAOProvider = DAOTestBase.getInstance();
    DAORegistry daoRegistry = DAORegistry.getInstance();
    templateDao = daoRegistry.getRootcauseTemplateDao();
  }

  @AfterMethod(alwaysRun = true)
  void afterMethod() {
    testDAOProvider.cleanup();
  }


  @Test
  public void testSaveOrUpdate() {
    RootcauseTemplateDTO template = constructTemplate(1);
    templateDao.saveOrUpdate(template);
    List<RootcauseTemplateDTO> res1 = templateDao.findAll();
    Assert.assertEquals(res1.size(), 1);
    Assert.assertEquals(res1.get(0).getName(), TEMPLATE_NAME + 1);
    Assert.assertEquals(res1.get(0).getApplication(), APPLICATION_NAME);
    template.setApplication(APPLICATION_NAME + "-1");
    templateDao.saveOrUpdate(template);
    List<RootcauseTemplateDTO> res2 = templateDao.findAll();
    Assert.assertEquals(res2.size(), 1);
    Assert.assertEquals(res2.get(0).getApplication(), APPLICATION_NAME + "-1");
  }

  @Test
  public void testFindByMetricId() {
    RootcauseTemplateDTO template1 = constructTemplate(1);
    templateDao.save(template1);
    RootcauseTemplateDTO template2 = constructTemplate(2);
    templateDao.save(template2);
    List<RootcauseTemplateDTO> res1 = templateDao.findAll();
    Assert.assertEquals(res1.size(), 2);
    List<RootcauseTemplateDTO> res2 = templateDao.findByMetricId(1);
    Assert.assertEquals(res2.size(), 1);
    Assert.assertEquals(res2.get(0).getName(), TEMPLATE_NAME + 1);
  }

  private RootcauseTemplateDTO constructTemplate(int metricId) {
    RootcauseTemplateDTO rootcauseTemplateDTO =  new RootcauseTemplateDTO();
    rootcauseTemplateDTO.setName(TEMPLATE_NAME + metricId);
    rootcauseTemplateDTO.setOwner("tester");
    rootcauseTemplateDTO.setApplication(APPLICATION_NAME);
    rootcauseTemplateDTO.setMetricId(metricId);
    rootcauseTemplateDTO.setModules(new ArrayList<>());
    return rootcauseTemplateDTO;
  }


}
