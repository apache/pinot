package com.linkedin.thirdeye.datalayer.bao;

import com.linkedin.thirdeye.datalayer.dto.EntityToEntityMappingDTO;
import com.linkedin.thirdeye.datalayer.pojo.EntityToEntityMappingBean.MappingType;

import java.util.List;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestEntityToEntityMappingManager extends AbstractManagerTestBase {
  Long testId1 = null;
  Long testId2 = null;
  Long testId3 = null;
  Long testId4 = null;
  String metricUrn1 = "thirdeye:metric:d1:m1";
  String metricUrn2 = "thirdeye:metric:d1:m2";
  String serviceUrn1 = "thirdeye:service:s1";
  String dimensionUrn1 = "thirdeye:dimension:country:UnitedStates";
  String dimensionUrn2 = "thirdeye:dimension:country:US";


  @BeforeClass
  void beforeClass() {
    super.init();
  }

  @AfterClass(alwaysRun = true)
  void afterClass() {
    super.cleanup();
  }

  @Test
  public void testCreate() {
    EntityToEntityMappingDTO dto = getTestEntityToEntityMapping(metricUrn1, metricUrn2, MappingType.METRIC_TO_METRIC);
    testId1 = entityToEntityMappingDAO.save(dto);
    Assert.assertNotNull(testId1);
    dto = getTestEntityToEntityMapping(metricUrn1, serviceUrn1, MappingType.METRIC_TO_SERVICE);
    testId2 = entityToEntityMappingDAO.save(dto);
    Assert.assertNotNull(testId2);
    dto = getTestEntityToEntityMapping(dimensionUrn1, dimensionUrn2, MappingType.DIMENSION_TO_DIMENSION);
    testId3 = entityToEntityMappingDAO.save(dto);
    Assert.assertNotNull(testId3);
  }

  @Test(dependsOnMethods = { "testCreate" })
  public void testFind() {
    EntityToEntityMappingDTO dto = entityToEntityMappingDAO.findById(testId1);
    Assert.assertEquals(dto.getId(), testId1);
    Assert.assertEquals(dto.getFromUrn(), metricUrn1);
    Assert.assertEquals(dto.getToUrn(), metricUrn2);
    Assert.assertEquals(dto.getMappingType(), MappingType.METRIC_TO_METRIC);

    List<EntityToEntityMappingDTO> list = entityToEntityMappingDAO.findAll();
    Assert.assertEquals(list.size(), 3);

    list = entityToEntityMappingDAO.findByFromUrn(metricUrn1);
    Assert.assertEquals(list.size(), 2);

    list = entityToEntityMappingDAO.findByToUrn(metricUrn2);
    Assert.assertEquals(list.size(), 1);

    dto = entityToEntityMappingDAO.findByFromAndToUrn(dimensionUrn1, dimensionUrn2);
    Assert.assertEquals(dto.getId(), testId3);

    list = entityToEntityMappingDAO.findByMappingType(MappingType.METRIC_TO_SERVICE);
    Assert.assertEquals(list.size(), 1);

    list = entityToEntityMappingDAO.findByFromUrnAndMappingType(metricUrn1, MappingType.METRIC_TO_METRIC);
    Assert.assertEquals(list.size(), 1);

    list = entityToEntityMappingDAO.findByToUrnAndMappingType(dimensionUrn2, MappingType.METRIC_TO_METRIC);
    Assert.assertEquals(list.size(), 0);
  }

  @Test(dependsOnMethods = { "testFind" })
  public void testUpdate() {
    EntityToEntityMappingDTO dto = entityToEntityMappingDAO.findById(testId1);
    Assert.assertEquals(dto.getScore(), 1d);
    dto.setScore(10);
    entityToEntityMappingDAO.update(dto);
    dto = entityToEntityMappingDAO.findById(testId1);
    Assert.assertEquals(dto.getScore(), 10d);
  }

  @Test(dependsOnMethods = { "testUpdate" })
  public void testDelete() {
    entityToEntityMappingDAO.deleteById(testId1);
    EntityToEntityMappingDTO dto = entityToEntityMappingDAO.findById(testId1);
    Assert.assertNull(dto);
  }
}
