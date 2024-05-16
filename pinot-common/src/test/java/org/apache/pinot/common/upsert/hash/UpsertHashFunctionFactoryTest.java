package org.apache.pinot.common.upsert.hash;

import org.apache.pinot.spi.config.table.HashFunction;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class UpsertHashFunctionFactoryTest {
  private int _numFunctionsChecked = 0;

  @Test
  public void testLoad() {
    validateFunctionLoaded(HashFunction.NONE);
    validateFunctionLoaded(HashFunction.MD5);
    validateFunctionLoaded(HashFunction.MURMUR3);
    assertEquals(_numFunctionsChecked, HashFunction.values().length);
  }

  private void validateFunctionLoaded(HashFunction hashFunction) {
    UpsertHashFunction upsertHashFunction = UpsertHashFunctionFactory.create(hashFunction.name());
    assertNotNull(upsertHashFunction);
    assertEquals(upsertHashFunction.getId(), hashFunction.name());
    _numFunctionsChecked++;
  }
}
