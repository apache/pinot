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

package org.apache.pinot.thirdeye.datalayer.bao;

import java.util.HashSet;
import org.apache.pinot.thirdeye.common.dimension.DimensionMap;
import org.apache.pinot.thirdeye.datalayer.dto.GroupedAnomalyResultsDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import java.util.ArrayList;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class TestGroupedAnomalyResultsManager {

  private DAOTestBase testDAOProvider;
  private GroupedAnomalyResultsManager groupedAnomalyResultsDAO;
  private MergedAnomalyResultManager mergedAnomalyResultDAO;
  @BeforeClass
  void beforeClass() {
    testDAOProvider = DAOTestBase.getInstance();
    DAORegistry daoRegistry = DAORegistry.getInstance();
    groupedAnomalyResultsDAO = daoRegistry.getGroupedAnomalyResultsDAO();
    mergedAnomalyResultDAO = daoRegistry.getMergedAnomalyResultDAO();
  }

  @AfterClass(alwaysRun = true)
  void afterClass() {
    testDAOProvider.cleanup();
  }

  @Test(dataProvider="groupedAnomalies")
  public void testGroupedResultCRUD(List<MergedAnomalyResultDTO> mergedAnomalyResultsSet1,
      List<MergedAnomalyResultDTO> mergedAnomalyResultsSet2) {

    DimensionMap dimensionMap = new DimensionMap();
    dimensionMap.put("D1", "K1");

    GroupedAnomalyResultsDTO groupedAnomalyResultsDTO = new GroupedAnomalyResultsDTO();
    groupedAnomalyResultsDTO.setAlertConfigId(1);
    groupedAnomalyResultsDTO.setDimensions(dimensionMap);
    groupedAnomalyResultsDTO.setAnomalyResults(mergedAnomalyResultsSet1);

    Long id = groupedAnomalyResultsDAO.save(groupedAnomalyResultsDTO);
    Assert.assertNotEquals(id, null);

    GroupedAnomalyResultsDTO groupedAnomalyResultsDTOByID = groupedAnomalyResultsDAO.findById(id);
    Assert.assertEquals(groupedAnomalyResultsDTOByID.getAnomalyResults(), groupedAnomalyResultsDTO.getAnomalyResults());
    Assert.assertEquals(groupedAnomalyResultsDTO.getEndTime(), 15);
    Assert.assertEquals(groupedAnomalyResultsDTO.getAlertConfigId(), 1);
    Assert.assertEquals(groupedAnomalyResultsDTO.getDimensions(), dimensionMap);
  }

  @Test(dataProvider="groupedAnomalies", dependsOnMethods = "testGroupedResultCRUD")
  public void testFindMostRecent(List<MergedAnomalyResultDTO> mergedAnomalyResultsSet1,
      List<MergedAnomalyResultDTO> mergedAnomalyResultsSet2) {

    DimensionMap dimensionMap = new DimensionMap();
    dimensionMap.put("D1", "K1");

    GroupedAnomalyResultsDTO groupedAnomalyResultsDTO2 = new GroupedAnomalyResultsDTO();
    groupedAnomalyResultsDTO2.setAlertConfigId(1);
    groupedAnomalyResultsDTO2.setDimensions(dimensionMap);
    groupedAnomalyResultsDTO2.setAnomalyResults(mergedAnomalyResultsSet2);
    Long id2 = groupedAnomalyResultsDAO.save(groupedAnomalyResultsDTO2);
    Assert.assertNotEquals(id2, null);

    GroupedAnomalyResultsDTO groupedAnomalyResultsDTO3 = new GroupedAnomalyResultsDTO();
    groupedAnomalyResultsDTO3.setAlertConfigId(1);
    groupedAnomalyResultsDTO3.setDimensions(dimensionMap);
    groupedAnomalyResultsDTO3.setAnomalyResults(mergedAnomalyResultsSet1);
    Long id3 = groupedAnomalyResultsDAO.save(groupedAnomalyResultsDTO3);

    GroupedAnomalyResultsDTO groupedAnomalyResultsDTOByID = groupedAnomalyResultsDAO.findById(id2);

    GroupedAnomalyResultsDTO recentGroupedAnomalyResultsDTO =
        groupedAnomalyResultsDAO.findMostRecentInTimeWindow(1, dimensionMap.toString(), 0, 50);
    Assert.assertNotEquals(recentGroupedAnomalyResultsDTO, null);
    Assert.assertEquals(recentGroupedAnomalyResultsDTO.getId(), groupedAnomalyResultsDTOByID.getId());
    Assert.assertEquals(recentGroupedAnomalyResultsDTO.getAnomalyResults(), groupedAnomalyResultsDTO2.getAnomalyResults());
  }

  @DataProvider(name = "groupedAnomalies")
  public Object[][] groupedAnomalies() {
    MergedAnomalyResultDTO mergedAnomalyResultDTO1 = new MergedAnomalyResultDTO();
    mergedAnomalyResultDTO1.setEndTime(10);
    mergedAnomalyResultDTO1.setChildIds(new HashSet<>());

    MergedAnomalyResultDTO mergedAnomalyResultDTO2 = new MergedAnomalyResultDTO();
    mergedAnomalyResultDTO2.setEndTime(15);
    mergedAnomalyResultDTO2.setChildIds(new HashSet<>());

    Long mergedAnomalyResultDTO1Id = mergedAnomalyResultDAO.save(mergedAnomalyResultDTO1);
    mergedAnomalyResultDTO1.setId(mergedAnomalyResultDTO1Id);
    Long mergedAnomalyResultDTO2Id = mergedAnomalyResultDAO.save(mergedAnomalyResultDTO2);
    mergedAnomalyResultDTO2.setId(mergedAnomalyResultDTO2Id);

    List<MergedAnomalyResultDTO> mergedAnomalyResultsSet1 = new ArrayList<>();
    mergedAnomalyResultsSet1.add(mergedAnomalyResultDTO2);
    mergedAnomalyResultsSet1.add(mergedAnomalyResultDTO1);

    MergedAnomalyResultDTO mergedAnomalyResultDTO3 = new MergedAnomalyResultDTO();
    mergedAnomalyResultDTO3.setEndTime(20);
    mergedAnomalyResultDTO3.setChildIds(new HashSet<>());

    MergedAnomalyResultDTO mergedAnomalyResultDTO4 = new MergedAnomalyResultDTO();
    mergedAnomalyResultDTO4.setEndTime(25);
    mergedAnomalyResultDTO4.setChildIds(new HashSet<>());

    Long mergedAnomalyResultDTO3Id = mergedAnomalyResultDAO.save(mergedAnomalyResultDTO3);
    mergedAnomalyResultDTO3.setId(mergedAnomalyResultDTO3Id);
    Long mergedAnomalyResultDTO4Id = mergedAnomalyResultDAO.save(mergedAnomalyResultDTO4);
    mergedAnomalyResultDTO4.setId(mergedAnomalyResultDTO4Id);

    List<MergedAnomalyResultDTO> mergedAnomalyResultsSet2 = new ArrayList<>();
    mergedAnomalyResultsSet2.add(mergedAnomalyResultDTO4);
    mergedAnomalyResultsSet2.add(mergedAnomalyResultDTO3);

    return new Object[][] {
        { mergedAnomalyResultsSet1, mergedAnomalyResultsSet2},
    };
  }
}
