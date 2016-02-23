package com.linkedin.thirdeye.query;

import com.google.common.collect.ImmutableList;
import com.linkedin.thirdeye.api.DimensionSpec;
import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;

public class TestThirdEyeQuery {
  private List<DimensionSpec> dimensions;
  private ThirdEyeQuery query;

  @BeforeClass
  public void beforeClass() {
    dimensions =
        ImmutableList.of(new DimensionSpec("A"), new DimensionSpec("B"), new DimensionSpec("C"));
  }

  @BeforeMethod
  public void beforeMethod() {
    query = new ThirdEyeQuery();
    query.setStart(new DateTime(1000));
    query.setEnd(new DateTime(2000));
  }

  @Test
  public void testGetDimensionCombinations_oneCombination() {
    query.addDimensionValue("A", "A1");
    query.addDimensionValue("B", "B1");
    query.addDimensionValue("C", "C1");
    List<String[]> combinations = query.getDimensionCombinations(dimensions);

    Assert.assertEquals(combinations.size(), 1);
    Assert.assertEquals(combinations.get(0), new String[] {
        "A1", "B1", "C1"
    });
  }

  @Test
  public void testGetDimensionCombinations_crossProduct() {
    query.addDimensionValue("A", "A1");
    query.addDimensionValue("A", "A2");
    query.addDimensionValue("B", "B1");
    query.addDimensionValue("B", "B2");
    query.addDimensionValue("C", "C1");
    List<String[]> combinations = query.getDimensionCombinations(dimensions);

    Assert.assertEquals(combinations.size(), 4);
    Assert.assertEquals(combinations.get(0), new String[] {
        "A1", "B1", "C1"
    });
    Assert.assertEquals(combinations.get(1), new String[] {
        "A1", "B2", "C1"
    });
    Assert.assertEquals(combinations.get(2), new String[] {
        "A2", "B1", "C1"
    });
    Assert.assertEquals(combinations.get(3), new String[] {
        "A2", "B2", "C1"
    });
  }

  @Test
  public void testGetDimensionCombinations_allStar() {
    List<String[]> combinations = query.getDimensionCombinations(dimensions);

    Assert.assertEquals(combinations.size(), 1);
    Assert.assertEquals(combinations.get(0), new String[] {
        "*", "*", "*"
    });
  }

  @Test
  public void testGetDimensionCombinations_someStar() {
    query.addDimensionValue("A", "A1");
    List<String[]> combinations = query.getDimensionCombinations(dimensions);

    Assert.assertEquals(combinations.size(), 1);
    Assert.assertEquals(combinations.get(0), new String[] {
        "A1", "*", "*"
    });
  }

  @Test
  public void testGetDimensionCombinations_someStarCrossProduct() {
    query.addDimensionValue("A", "A1");
    query.addDimensionValue("A", "A2");
    List<String[]> combinations = query.getDimensionCombinations(dimensions);

    Assert.assertEquals(combinations.size(), 2);
    Assert.assertEquals(combinations.get(0), new String[] {
        "A1", "*", "*"
    });
    Assert.assertEquals(combinations.get(1), new String[] {
        "A2", "*", "*"
    });
  }
}
