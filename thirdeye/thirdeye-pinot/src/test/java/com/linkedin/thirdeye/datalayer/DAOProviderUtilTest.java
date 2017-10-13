package com.linkedin.thirdeye.datalayer;

import com.linkedin.thirdeye.datalayer.bao.DAOTestBase;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.jdbc.MergedAnomalyResultManagerImpl;
import com.linkedin.thirdeye.datalayer.util.DaoProviderUtil;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class DAOProviderUtilTest {
  private DaoProvider DAO_REGISTRY;
  @BeforeClass
  public void beforeClass() {
    DAO_REGISTRY = DAOTestBase.getInstance();
  }

  @AfterClass(alwaysRun = true)
  public void afterClass() {
    DAO_REGISTRY.restart();
  }

  @Test
  public void testProviderReturnsSameInstance() {
    MergedAnomalyResultManager m1 = DAO_REGISTRY.getMergedAnomalyResultDAO();
    MergedAnomalyResultManager m2 = DAO_REGISTRY.getMergedAnomalyResultDAO();
    Assert.assertSame(m1, m2);
  }
}
