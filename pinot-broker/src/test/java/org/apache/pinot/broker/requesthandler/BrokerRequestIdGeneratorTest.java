package org.apache.pinot.broker.requesthandler;

import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class BrokerRequestIdGeneratorTest {

  @Test
  public void testGet() {
    BrokerRequestIdGenerator gen = new BrokerRequestIdGenerator("foo");
    long id = gen.get();
    assertEquals(id % 10, 0);
    assertEquals(id / 10 % 10, 0);

    id = gen.get();
    assertEquals(id % 10, 0);
    assertEquals(id / 10 % 10, 1);
  }
}