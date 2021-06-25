package org.apache.pinot.segment.local.upsert.merger;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class PartialUpsertMergerTest {

  @Test
  public void testAppendMergers() {
    AppendMerger appendMerger = new AppendMerger();

    Integer array1[] = {1, 2, 3};
    Integer array2[] = {3, 4, 6};

    assertEquals(new Integer[]{1, 2, 3, 3, 4, 6}, appendMerger.merge(array1, array2));
  }

  @Test
  public void testIncrementMergers() {
    IncrementMerger incrementMerger = new IncrementMerger();
    assertEquals(3, incrementMerger.merge(1, 2));
  }

  @Test
  public void testOverwriteMergers() {
    OverwriteMerger overwriteMerger = new OverwriteMerger();
    assertEquals("newValue", overwriteMerger.merge("oldValue", "newValue"));
  }

  @Test
  public void testUnionMergers() {
    UnionMerger unionMerger = new UnionMerger();

    String array1[] = {"a", "b", "c"};
    String array2[] = {"c", "d", "e"};

    assertEquals(new String[]{"a", "b", "c", "d", "e"}, unionMerger.merge(array1, array2));
  }
}
