package com.linkedin.thirdeye.util;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;

@Test
public class ThirdEyeUtilsTest {

  @Test(dataProvider = "testSortedFiltersDataProvider")
  public void testSortedFilters(String filters, String expectedFilters) {
    String sortedFilters = ThirdEyeUtils.getSortedFilters(filters);
    Assert.assertEquals(sortedFilters, expectedFilters);
  }

  @Test(dataProvider = "testSortedFiltersFromJsonDataProvider")
  public void testSortedFiltersFromJson(String filterJson, String expectedFilters) {
    String sortedFilters = ThirdEyeUtils.getSortedFiltersFromJson(filterJson);
    Assert.assertEquals(sortedFilters, expectedFilters);
  }

  @Test(dataProvider = "testSortedFiltersFromMultimapDataProvider")
  public void testSortedFiltersFromMultimap(Multimap<String, String> filterMultimap,
      String expectedFilters) {
    String sortedFilters = ThirdEyeUtils.getSortedFiltersFromMultiMap(filterMultimap);
    Assert.assertEquals(sortedFilters, expectedFilters);
  }

  @DataProvider(name = "testSortedFiltersDataProvider")
  public Object[][] testSortedFiltersDataProvider() {
    return new Object[][] {
        {
            "a=z;z=d;a=f;a=e;k=m;k=f;z=c;f=g;", "a=e;a=f;a=z;f=g;k=f;k=m;z=c;z=d"
        }, {
            ";", null
        }, {
            "a=b", "a=b"
        }, {
            "a=z;a=b", "a=b;a=z"
        }
    };
  }

  @DataProvider(name = "testSortedFiltersFromJsonDataProvider")
  public Object[][] testSortedFiltersFromJsonDataProvider() {
    return new Object[][] {
        {
            "{\"a\":[\"b\",\"c\"]}", "a=b;a=c"
        }, {
            "{\"z\":[\"g\"],\"x\":[\"l\"],\"a\":[\"b\",\"c\"]}", "a=b;a=c;x=l;z=g"
        }
    };
  }

  @DataProvider(name = "testSortedFiltersFromMultimapDataProvider")
  public Object[][] testSortedFiltersFromMultimapDataProvider() {
    ListMultimap<String, String> multimap1 = ArrayListMultimap.create();
    multimap1.put("a", "b");
    multimap1.put("a", "c");

    ListMultimap<String, String> multimap2 = ArrayListMultimap.create();
    multimap2.put("a", "b");
    multimap2.put("z", "g");
    multimap2.put("k", "b");
    multimap2.put("i", "c");
    multimap2.put("a", "c");

    return new Object[][] {
        {
            multimap1, "a=b;a=c"
        }, {
            multimap2, "a=b;a=c;i=c;k=b;z=g"
        }
    };
  }

}
