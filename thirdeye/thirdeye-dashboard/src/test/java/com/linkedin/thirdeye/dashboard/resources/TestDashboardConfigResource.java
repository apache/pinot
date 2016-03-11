package com.linkedin.thirdeye.dashboard.resources;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.concurrent.ConcurrentUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.client.ThirdEyeRequest;
import com.linkedin.thirdeye.dashboard.api.CollectionSchema;
import com.linkedin.thirdeye.dashboard.api.QueryResult;
import com.linkedin.thirdeye.dashboard.util.DataCache;
import com.linkedin.thirdeye.dashboard.util.QueryCache;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class TestDashboardConfigResource {
  private static final String COLLECTION = "ignored";

  private DataCache mockDataCache;
  private QueryCache mockQueryCache;
  private CollectionSchema mockCollectionSchema;
  private final ObjectMapper objectMapper = new ObjectMapper();

  DashboardConfigResource testDashboardConfigResource;

  @BeforeMethod
  public void beforeMethod() throws Exception {
    mockDataCache = mock(DataCache.class);
    mockCollectionSchema = mock(CollectionSchema.class);
    mockQueryCache = mock(QueryCache.class);

    when(mockDataCache.getCollectionSchema(COLLECTION)).thenReturn(mockCollectionSchema);

    testDashboardConfigResource =
        new DashboardConfigResource(mockDataCache, mockQueryCache, null, null, null, objectMapper);
  }

  @Test
  public void testGetAllDimensions() throws Exception {
    testDashboardConfigResource.getAllDimensions(COLLECTION);
    verify(mockCollectionSchema).getDimensions();
  }

  @Test
  public void testGetDimensionsWithSelection() throws Exception {
    UriInfo mockUriInfo = mock(UriInfo.class);
    MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
    queryParams.put("b", Arrays.asList("x", "y", "z"));
    when(mockUriInfo.getQueryParameters()).thenReturn(queryParams);
    when(mockCollectionSchema.getDimensions()).thenReturn(Arrays.asList("a", "b", "c"));
    Assert.assertEquals(testDashboardConfigResource.getDimensions(COLLECTION, mockUriInfo),
        Arrays.asList("a", "c"));
  }

  @Test
  public void testGetAllDimensionAliases() throws Exception {
    testDashboardConfigResource.getDimensionAliases(COLLECTION);
    verify(mockCollectionSchema).getDimensionAliases();
  }

  @Test
  public void testGetDimensionValues() throws Exception {
    // TODO lots of moving parts here, difficult to test easily.
    long baselineMillis = 0;
    long currentMillis = TimeUnit.DAYS.toMillis(7);
    List<String> dimensions = Arrays.asList("a");
    when(mockCollectionSchema.getDimensions()).thenReturn(dimensions);
    String metric = "__COUNT";
    List<String> metrics = Collections.singletonList(metric);
    when(mockCollectionSchema.getMetrics()).thenReturn(metrics);
    List<String> values1 = Arrays.asList("v1", "v3");
    List<String> values2 = Arrays.asList("v2", "v4");
    Set<String> expectedValues = new HashSet<>(values1);
    expectedValues.addAll(values2);

    Future<QueryResult> result1 =
        createQueryResultFuture(dimensions, metrics, values1, baselineMillis);
    Future<QueryResult> result2 =
        createQueryResultFuture(dimensions, metrics, values2, currentMillis);
    when(mockQueryCache.getQueryResultAsync(any(ThirdEyeRequest.class))).thenReturn(result1,
        result2);
    Map<String, Collection<String>> dimensionValues = testDashboardConfigResource
        .getDimensionValues(COLLECTION, baselineMillis, currentMillis, 0.0, expectedValues.size());

    Assert.assertTrue(
        CollectionUtils.isEqualCollection(dimensionValues.get(dimensions.get(0)), expectedValues));
  }

  // test method for generating singleton results, using dimension values for each
  // dimension with default values of 1. No guarantees on reusability here.
  private Future<QueryResult> createQueryResultFuture(List<String> dimensions, List<String> metrics,
      List<String> dimensionValues, long timestamp) {
    QueryResult result = new QueryResult();
    result.setDimensions(dimensions);
    result.setMetrics(metrics);
    Map<String, Map<String, Number[]>> data = new HashMap<String, Map<String, Number[]>>();
    for (String dimensionValue : dimensionValues) {
      HashMap<String, Number[]> entry = new HashMap<>();
      data.put("[\"" + dimensionValue + "\"]", entry);
      Number[] values = new Number[metrics.size()];
      Arrays.fill(values, 1.0);
      entry.put(Long.toString(timestamp), values);
    }
    result.setData(data);
    return ConcurrentUtils.constantFuture(result);
  }

  @Test
  public void testGetMetrics() throws Exception {
    testDashboardConfigResource.getMetrics(COLLECTION);
    verify(mockCollectionSchema).getMetrics();
  }

  @Test
  public void testGetAllMetricAliases() throws Exception {
    testDashboardConfigResource.getMetricAliases(COLLECTION);
    verify(mockCollectionSchema).getMetricAliases();
  }

  @Test
  public void testGetMetricAliasesAsList() throws Exception {
    testDashboardConfigResource.getMetricAliasesAsList(COLLECTION);
    verify(mockCollectionSchema).getMetricAliases();
  }

}
