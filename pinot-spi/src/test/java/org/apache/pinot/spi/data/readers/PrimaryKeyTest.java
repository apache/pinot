package org.apache.pinot.spi.data.readers;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;


public class PrimaryKeyTest {

  @Test
  public void testPrimaryKeyComparison() {
    PrimaryKey left = new PrimaryKey(new Object[]{"111", 2});
    PrimaryKey right = new PrimaryKey(new Object[]{"111", 2});
    assertEquals(left, right);
    assertEquals(left.hashCode(), right.hashCode());

    right = new PrimaryKey(new Object[]{"222", 2});
    assertNotEquals(left, right);
    assertNotEquals(left.hashCode(), right.hashCode());
  }
}
