package com.linkedin.thirdeye.datalayer;

import com.linkedin.thirdeye.datalayer.bao.DAOTestBase;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datasource.DAORegistry;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class DAOProviderUtilTest {
  private DAOTestBase testDAOProvider;
  @BeforeClass
  public void beforeClass() {
    testDAOProvider = DAOTestBase.getInstance();
  }

  @AfterClass(alwaysRun = true)
  public void afterClass() {
    testDAOProvider.cleanup();
  }

  @Test
  public void testProviderReturnsSameInstance() {
    DAORegistry daoRegistry = DAORegistry.getInstance();
    MergedAnomalyResultManager m1 = daoRegistry.getMergedAnomalyResultDAO();
    MergedAnomalyResultManager m2 = daoRegistry.getMergedAnomalyResultDAO();
    Assert.assertSame(m1, m2);
  }
}
