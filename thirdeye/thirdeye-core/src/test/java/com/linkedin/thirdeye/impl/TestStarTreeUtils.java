package com.linkedin.thirdeye.impl;

import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.DimensionSpec;
import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.api.SplitSpec;
import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeQuery;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestStarTreeUtils {
  private StarTreeConfig config;
  private MetricSchema metricSchema;

  @BeforeClass
  public void beforeClass() throws Exception {
    config = StarTreeConfig.decode(ClassLoader.getSystemResourceAsStream("sample-config.yml"));
    metricSchema = MetricSchema.fromMetricSpecs(config.getMetrics());
  }

  @Test
  public void testFilterQueries() throws Exception {
    StarTreeConfig config =
        new StarTreeConfig.Builder().setCollection("dummy").setSplit(new SplitSpec(4, null))
            .setMetrics(Arrays.asList(new MetricSpec("M", MetricType.INT)))
            .setDimensions(Arrays.asList(new DimensionSpec("A"), new DimensionSpec("B"),
                new DimensionSpec("C")))
        .setRecordStoreFactoryClass(
            StarTreeRecordStoreFactoryLogBufferImpl.class.getCanonicalName()).build();

    StarTree starTree = new StarTreeImpl(config);
    starTree.open();

    for (int i = 0; i < 100; i++) {
      MetricTimeSeries ts = new MetricTimeSeries(metricSchema);
      ts.set(i, "M", 1);

      StarTreeRecordImpl.Builder builder = new StarTreeRecordImpl.Builder();
      builder.setDimensionKey(getDimensionKey("A" + (i % 2), "B" + (i % 4), "C" + (i % 8)));
      builder.setMetricTimeSeries(ts);
      starTree.add(builder.build(config));
    }

    StarTreeQuery baseQuery = new StarTreeQueryImpl.Builder()
        .setDimensionKey(
            getDimensionKey(StarTreeConstants.ALL, StarTreeConstants.STAR, StarTreeConstants.STAR))
        .build(config);

    List<StarTreeQuery> queries = StarTreeUtils.expandQueries(starTree, baseQuery);
    Assert.assertEquals(queries.size(), 3); // A0 and A1, and "?"

    Map<String, List<String>> filter = new HashMap<String, List<String>>();
    filter.put("A", Arrays.asList("A0"));

    List<StarTreeQuery> filteredQueries = StarTreeUtils.filterQueries(config, queries, filter);
    Assert.assertEquals(filteredQueries.size(), 1);
    Assert.assertEquals(
        filteredQueries.get(0).getDimensionKey().getDimensionValue(config.getDimensions(), "A"),
        "A0");

    starTree.close();
  }

  private DimensionKey getDimensionKey(String a, String b, String c) {
    return new DimensionKey(new String[] {
        a, b, c
    });
  }
}
