package com.linkedin.pinot.core.indexsegment;

import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Test for IndexType.
 *
 * @author jfim
 */
public class IndexTypeTest {
  @Test
  public void testValueOfStr() {
    Assert.assertEquals(IndexType.valueOfStr("columnar"), IndexType.COLUMNAR);
    Assert.assertEquals(IndexType.valueOfStr("simple"), IndexType.SIMPLE);

    try {
      IndexType.valueOfStr("this does not exist");

      // Should throw an exception
      Assert.fail();
    } catch (Exception e) {
      // Success!
    }
  }
}
