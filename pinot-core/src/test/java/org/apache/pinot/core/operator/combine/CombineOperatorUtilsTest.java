package org.apache.pinot.core.operator.combine;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * Unit test for {@link CombineOperatorUtils}
 */
public class CombineOperatorUtilsTest {

  public static final String PINOT_SERVER_MAX_THREADS_PER_QUERY = "pinot.server.maxThreadsPerQuery";

  @Test
  public void testGetMaxThreadsPerQuery() {
    System.setProperty(PINOT_SERVER_MAX_THREADS_PER_QUERY, "4");
    assertEquals(CombineOperatorUtils.MAX_NUM_THREADS_PER_QUERY, 4);
  }
}