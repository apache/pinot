/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.broker.broker;

import com.linkedin.pinot.broker.requesthandler.BrokerRequestHandler;
import com.linkedin.pinot.broker.routing.TimeBoundaryService;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.pql.parsers.Pql2Compiler;
import java.io.File;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Test for BrokerRequest Validation:
 * - Ensures query response limit is honored while validating a request.
 */
public class BrokerRequestValidationTest {

  private static final int QUERY_RESPONSE_LIMIT = 100;
  private static final String QUERY_RESPONSE_LIMIT_CONFIG = "pinot.broker.query.response.limit";
  private static final CharSequence LIMIT_VALIDATION_STRING = "exceeded maximum allowed value";
  private BrokerServerBuilder brokerBuilder;

  /**
   * Setup method to start the broker. Stars the broker with
   * a query response limit of {@value QUERY_RESPONSE_LIMIT}
   * @return
   * @throws Exception
   */
  @BeforeClass
  public BrokerServerBuilder setup()
      throws Exception {
    // Read default configurations.
    PropertiesConfiguration config = new PropertiesConfiguration(
        new File(BrokerServerBuilderTest.class.getClassLoader().getResource("broker.properties").toURI()));

    // Create a dummy time boundary service
    TimeBoundaryService timeBoundaryService = new TimeBoundaryService() {
      @Override
      public TimeBoundaryInfo getTimeBoundaryInfoFor(String table) {
        return null;
      }

      @Override
      public void remove(String tableName) {
        return;
      }
    };

    // Set the value for query response limit.
    config.addProperty(QUERY_RESPONSE_LIMIT_CONFIG, QUERY_RESPONSE_LIMIT);
    brokerBuilder = new BrokerServerBuilder(config, null, timeBoundaryService, null);
    brokerBuilder.buildNetwork();
    brokerBuilder.buildHTTP();
    brokerBuilder.start();

    return brokerBuilder;
  }

  /**
   * Teardown method to stop the broker.
   *
   * @throws Exception
   */
  @AfterClass
  public void tearDown()
      throws Exception {
    brokerBuilder.stop();
  }

  /**
   * Test to ensure that values for 'TOP' smaller than or equal to the
   * configured query response limit are validated, and others are not.
   *
   * @throws Exception
   */
  @Test
  public void testTop()
      throws Exception {

    String baseQuery = "select count(*) from myTable group by myColumn TOP ";
    String query = baseQuery + QUERY_RESPONSE_LIMIT;
    String exceptionMessage = runQuery(query);
    Assert.assertNull(exceptionMessage);

    query = baseQuery + (QUERY_RESPONSE_LIMIT + 1);
    exceptionMessage = runQuery(query);
    Assert.assertTrue((exceptionMessage != null) && exceptionMessage.contains(LIMIT_VALIDATION_STRING));
  }

  /**
   * Test to ensure that values for 'TOP' smaller than or equal to the
   * configured query response limit are validated, and others are not.
   *
   * @throws Exception
   */
  @Test
  public void testLimit()
      throws Exception {

    String baseQuery = "select * from myTable limit ";

    String query = baseQuery + QUERY_RESPONSE_LIMIT;
    String exceptionMessage = runQuery(query);
    Assert.assertNull(exceptionMessage);

    query = baseQuery + (QUERY_RESPONSE_LIMIT + 1);
    exceptionMessage = runQuery(query);
    Assert.assertTrue((exceptionMessage != null) && exceptionMessage.contains(LIMIT_VALIDATION_STRING));
  }

  /**
   * Helper method to run the query.
   *
   * @param query
   * @return Returns exception string if any, null otherwise.
   * @throws Exception
   */
  private String runQuery(String query)
      throws Exception {

    BrokerRequestHandler brokerRequestHandler = brokerBuilder.getBrokerRequestHandler();
    Pql2Compiler compiler = new Pql2Compiler();

    BrokerRequest brokerRequest = compiler.compileToBrokerRequest(query);

    String exceptionMessage = null;
    try {
      brokerRequestHandler.validateRequest(brokerRequest);
    } catch (Exception e) {
      exceptionMessage = e.getMessage();
    }
    return exceptionMessage;
  }
}
