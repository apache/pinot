package com.linkedin.thirdeye.datalayer;

import com.linkedin.thirdeye.datalayer.bao.DAOTestBase;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class DAOProviderUtilTest {
  private DaoProvider testDAOProvider;
  @BeforeClass
  public void beforeClass() {
    testDAOProvider = DAOTestBase.getInstance();
  }

  @AfterClass(alwaysRun = true)
  public void afterClass() {
    testDAOProvider.restart();
  }

  @Test
  public void testProviderReturnsSameInstance() {
    MergedAnomalyResultManager m1 = testDAOProvider.getMergedAnomalyResultDAO();
    MergedAnomalyResultManager m2 = testDAOProvider.getMergedAnomalyResultDAO();
    Assert.assertSame(m1, m2);
  }
}
