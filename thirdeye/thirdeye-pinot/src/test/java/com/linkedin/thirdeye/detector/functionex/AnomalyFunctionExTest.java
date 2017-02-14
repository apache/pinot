package com.linkedin.thirdeye.detector.functionex;

import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionExDTO;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;


public class AnomalyFunctionExTest {

  static final String MOCK_CLASS_NAME = "com.linkedin.thirdeye.detector.functionex.AnomalyFunctionExTest$MockAnomalyFunctionEx";

  static final String DATASOURCE = "MOCK";

  static final String CONFIG_MESSAGE = "message";
  static final String CONFIG_QUERY = "query";

  static final String MESSAGE_DEFAULT = "DEFAULT MESSAGE";
  static final String MESSAGE_CUSTOM = "CUSTOM MESSAGE";

  static final String QUERY_DEFAULT = "DEFAULT QUERY";
  static final String QUERY_CUSTOM = "CUSTOM QUERY";

  static class MockDataSource implements AnomalyFunctionExDataSource<String, String> {
    String query = null;

    @Override
    public String query(String query, AnomalyFunctionExContext context) throws Exception {
      this.query = query;
      return query;
    }
  }

  static class MockAnomalyFunctionEx extends AnomalyFunctionEx {
    @Override
    public AnomalyFunctionExResult apply() throws Exception {
      String message = getConfig(CONFIG_MESSAGE, MESSAGE_DEFAULT);
      if(hasDataSource(DATASOURCE))
        message = queryDataSource(DATASOURCE, getConfig(CONFIG_QUERY, QUERY_DEFAULT));
      AnomalyFunctionExResult result = new AnomalyFunctionExResult();
      result.setAnomaly(true);
      result.setMessage(message);
      return result;
    }
  }

  @Test
  public void testInstantiation() throws Exception {
    AnomalyFunctionExDTO dto = new AnomalyFunctionExDTO();
    dto.setClassName(MOCK_CLASS_NAME);
    dto.setConfig(Collections.EMPTY_MAP);

    AnomalyFunctionExFactory factory = new AnomalyFunctionExFactory();

    Assert.assertNotNull(factory.fromSpec(dto));
  }

  @Test
  public void testConfigInjection() throws Exception {
    AnomalyFunctionExDTO dto = new AnomalyFunctionExDTO();
    dto.setClassName(MOCK_CLASS_NAME);
    dto.setConfig(Collections.singletonMap(CONFIG_MESSAGE, MESSAGE_CUSTOM));

    AnomalyFunctionExFactory factory = new AnomalyFunctionExFactory();

    AnomalyFunctionEx func = factory.fromSpec(dto);

    Assert.assertEquals(func.apply().getMessage(), MESSAGE_CUSTOM);
  }

  @Test
  public void testConfigInjectionTolerateNonExistingWithDefault() throws Exception {
    AnomalyFunctionExDTO dto = new AnomalyFunctionExDTO();
    dto.setClassName(MOCK_CLASS_NAME);
    dto.setConfig(Collections.EMPTY_MAP);

    AnomalyFunctionExFactory factory = new AnomalyFunctionExFactory();


    AnomalyFunctionEx func = factory.fromSpec(dto);

    Assert.assertEquals(func.apply().getMessage(), MESSAGE_DEFAULT);
  }

  @Test
  public void testDataSourceInjection() throws Exception {
    AnomalyFunctionExDTO dto = new AnomalyFunctionExDTO();
    dto.setClassName(MOCK_CLASS_NAME);
    dto.setConfig(Collections.singletonMap(CONFIG_QUERY, QUERY_CUSTOM));

    MockDataSource ds = new MockDataSource();

    AnomalyFunctionExFactory factory = new AnomalyFunctionExFactory();
    factory.addDataSource(DATASOURCE, ds);

    AnomalyFunctionEx func = factory.fromSpec(dto);

    func.apply();

    Assert.assertEquals(ds.query, QUERY_CUSTOM);
  }

  @Test
  public void testEndToEnd() throws Exception {
    Map<String, String> config = new HashMap<>();
    config.put(CONFIG_MESSAGE, MESSAGE_CUSTOM);
    config.put(CONFIG_QUERY, QUERY_CUSTOM);

    AnomalyFunctionExDTO dto = new AnomalyFunctionExDTO();
    dto.setClassName(MOCK_CLASS_NAME);
    dto.setConfig(config);

    MockDataSource ds = new MockDataSource();

    AnomalyFunctionExFactory factory = new AnomalyFunctionExFactory();
    factory.addDataSource(DATASOURCE, ds);

    AnomalyFunctionEx func = factory.fromSpec(dto);

    AnomalyFunctionExResult result = func.apply();

    Assert.assertEquals(ds.query, QUERY_CUSTOM);

    Assert.assertEquals(result.getMessage(), QUERY_CUSTOM);
    Assert.assertEquals(result.isAnomaly(), true);
  }

}
