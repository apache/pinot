package com.linkedin.thirdeye.db;

import com.linkedin.thirdeye.common.persistence.PersistenceUtil;
import com.linkedin.thirdeye.db.dao.AnomalyFunctionDAO;
import com.linkedin.thirdeye.db.dao.AnomalyResultDAO;
import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import org.testng.annotations.BeforeClass;

public abstract class AbstractDbTestBase {
  protected AnomalyFunctionDAO anomalyFunctionDAO;
  protected AnomalyResultDAO anomalyResultDAO;

  @BeforeClass(alwaysRun = true)
  public void init() throws URISyntaxException {
    URL url = AbstractDbTestBase.class.getResource("/persistence.yml");
    File configFile = new File(url.toURI());
    PersistenceUtil.init(configFile);
    anomalyFunctionDAO = PersistenceUtil.getInstance(AnomalyFunctionDAO.class);
    anomalyResultDAO = PersistenceUtil.getInstance(AnomalyResultDAO.class);
  }
}
