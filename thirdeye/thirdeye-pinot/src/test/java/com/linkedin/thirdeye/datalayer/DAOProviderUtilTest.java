package com.linkedin.thirdeye.datalayer;

import com.linkedin.thirdeye.datalayer.bao.AbstractManagerTestBase;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.jdbc.MergedAnomalyResultManagerImpl;
import com.linkedin.thirdeye.datalayer.util.DaoProviderUtil;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class DAOProviderUtilTest extends AbstractManagerTestBase {
  @BeforeClass
  public void beforeClass() {
    super.init();
  }

  @AfterClass(alwaysRun = true)
  public void afterClass() {
    super.cleanup();
  }

  @Test
  public void testProviderReturnsSameInstance() {
    MergedAnomalyResultManager m1 = DaoProviderUtil.getInstance(MergedAnomalyResultManagerImpl.class);
    MergedAnomalyResultManager m2 = DaoProviderUtil.getInstance(MergedAnomalyResultManagerImpl.class);
    Assert.assertSame(m1, m2);
  }
}
