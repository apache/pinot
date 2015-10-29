package com.linkedin.thirdeye.dashboard.resources;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.dashboard.api.CollectionSchema;
import com.linkedin.thirdeye.dashboard.util.DataCache;
import com.linkedin.thirdeye.dashboard.util.QueryCache;

public class TestDashboardConfigResource {
  private static final String SERVER_URI = "ignored";
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

    when(mockDataCache.getCollectionSchema(SERVER_URI, COLLECTION))
        .thenReturn(mockCollectionSchema);

    testDashboardConfigResource =
        new DashboardConfigResource(SERVER_URI, mockDataCache, mockQueryCache, objectMapper);
  }

  @Test
  public void testGetDimensions() throws Exception {
    testDashboardConfigResource.getDimensions(COLLECTION);
    verify(mockCollectionSchema).getDimensions();
  }

  @Test
  public void testGetDimensionAliases() throws Exception {
    testDashboardConfigResource.getDimensionAliases(COLLECTION);
    verify(mockCollectionSchema).getDimensionAliases();
  }

  @Test
  public void testGetDimensionValues() throws Exception {
    // TODO lots of moving parts here, difficult to test easily.
  }

  @Test
  public void testGetMetrics() throws Exception {
    testDashboardConfigResource.getMetrics(COLLECTION);
    verify(mockCollectionSchema).getMetrics();
  }

  @Test
  public void testGetMetricAliases() throws Exception {
    testDashboardConfigResource.getMetricAliases(COLLECTION);
    verify(mockCollectionSchema).getMetricAliases();
  }

}
