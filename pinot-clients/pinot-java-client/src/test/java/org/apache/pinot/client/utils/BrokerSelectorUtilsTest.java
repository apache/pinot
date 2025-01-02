/**
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
package org.apache.pinot.client.utils;

import java.util.HashMap;
import java.util.List;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;


public class BrokerSelectorUtilsTest {

  HashMap<String, List<String>> _brokerData = new HashMap<>();
  @Test
  public void getTablesCommonBrokersSetNullTables() {
    Set<String> tableSet = BrokerSelectorUtils.getTablesCommonBrokersSet(null, _brokerData);
    Assert.assertEquals(tableSet, Set.of());
  }

  @Test
  public void getTablesCommonBrokersListNullTables() {
    List<String> tableList = BrokerSelectorUtils.getTablesCommonBrokers(null, _brokerData);
    Assert.assertNull(tableList);
  }

  @Test
  public void getTablesCommonBrokersSetEmptyTables() {
    Set<String> tableSet = BrokerSelectorUtils.getTablesCommonBrokersSet(List.of(), _brokerData);
    Assert.assertEquals(tableSet, Set.of());
  }

  @Test
  public void getTablesCommonBrokersListEmptyTables() {
    List<String> tableList = BrokerSelectorUtils.getTablesCommonBrokers(List.of(), _brokerData);
    Assert.assertNull(tableList);
  }

  @Test
  public void getTablesCommonBrokersSetNotExistentTable() {
    Set<String> tableSet = BrokerSelectorUtils.getTablesCommonBrokersSet(List.of("notExistent"), _brokerData);
    Assert.assertEquals(tableSet, Set.of());
  }

  @Test
  public void getTablesCommonBrokersListNotExistentTable() {
    List<String> tableList = BrokerSelectorUtils.getTablesCommonBrokers(List.of("notExistent"), _brokerData);
    Assert.assertNull(tableList);
  }

  @Test
  public void getTablesCommonBrokersSetOneTable() {
    _brokerData.put("table1", List.of("broker1"));
    Set<String> tableSet = BrokerSelectorUtils.getTablesCommonBrokersSet(List.of("table1"), _brokerData);
    Assert.assertEquals(tableSet, Set.of("broker1"));
  }

  @Test
  public void getTablesCommonBrokersListOneTable() {
    _brokerData.put("table1", List.of("broker1"));
    List<String> tableList = BrokerSelectorUtils.getTablesCommonBrokers(List.of("table1"), _brokerData);
    Assert.assertNotNull(tableList);
    Assert.assertEquals(tableList, List.of("broker1"));
  }

  @Test
  public void getTablesCommonBrokersSetTwoTables() {
    _brokerData.put("table1", List.of("broker1"));
    _brokerData.put("table2", List.of("broker1"));
    Set<String> tableSet = BrokerSelectorUtils.getTablesCommonBrokersSet(List.of("table1", "table2"), _brokerData);
    Assert.assertNotNull(tableSet);
    Assert.assertEquals(tableSet, Set.of("broker1"));
  }

  @Test
  public void getTablesCommonBrokersListTwoTables() {
    _brokerData.put("table1", List.of("broker1"));
    _brokerData.put("table2", List.of("broker1"));
    List<String> tableList = BrokerSelectorUtils.getTablesCommonBrokers(List.of("table1", "table2"), _brokerData);
    Assert.assertNotNull(tableList);
    Assert.assertEquals(tableList, List.of("broker1"));
  }

  @Test
  public void getTablesCommonBrokersSetTwoTablesDifferentBrokers() {
    _brokerData.put("table1", List.of("broker1"));
    _brokerData.put("table2", List.of("broker2"));
    Set<String> tableSet = BrokerSelectorUtils.getTablesCommonBrokersSet(List.of("table1", "table2"), _brokerData);
    Assert.assertEquals(tableSet, Set.of());
  }

  @Test
  public void getTablesCommonBrokersListTwoTablesDifferentBrokers() {
    _brokerData.put("table1", List.of("broker1"));
    _brokerData.put("table2", List.of("broker2"));
    List<String> tableList = BrokerSelectorUtils.getTablesCommonBrokers(List.of("table1", "table2"), _brokerData);
    Assert.assertNull(tableList);
  }

  @AfterMethod
  public void tearDown() {
    _brokerData.clear();
  }
}
