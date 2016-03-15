package com.linkedin.thirdeye.client;

import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.mockito.ArgumentMatcher;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.collect.LinkedListMultimap;
import com.linkedin.thirdeye.client.CollectionMapThirdEyeClient.CollectionKey;
import com.linkedin.thirdeye.client.util.ThirdEyeClientConfig;

public class TestCollectionMapThirdEyeClient {
  private static final String PREFIX_C = "C";
  private static final String PREFIX_AB = "AB";
  private static final String COLLECTION_A = "A";
  private static final String COLLECTION_B = "B";
  private static final String COLLECTION_C = PREFIX_C;
  private CollectionMapThirdEyeClient clientMap;
  private ThirdEyeClient clientAB;
  private ThirdEyeClient clientC;
  private List<ThirdEyeClient> clients;
  private Map<ThirdEyeClient, String> prefixMap;

  // @BeforeMethod pseudo before-method call for most methods.
  private void initMap() throws Exception {
    clientAB = mock(ThirdEyeClient.class, "clientAB");
    when(clientAB.getCollections()).thenReturn(Arrays.asList(COLLECTION_A, COLLECTION_B));

    clientC = mock(ThirdEyeClient.class, "clientC");
    when(clientC.getCollections()).thenReturn(Arrays.asList(COLLECTION_C));

    clientMap = new CollectionMapThirdEyeClient();
    clientMap.add(clientAB, PREFIX_AB);
    clientMap.add(clientC, PREFIX_C);

    prefixMap = new HashMap<ThirdEyeClient, String>();
    prefixMap.put(clientAB, PREFIX_AB);
    prefixMap.put(clientC, PREFIX_C);

    clients = Arrays.asList(clientAB, clientC);
  }

  @Test
  public void getClients() throws Exception {
    initMap();
    Assert.assertEquals(clientMap.getClients(), clients);
  }

  @Test(dataProvider = "collectionClientProvider")
  public void getByCollection(String collectionKey, ThirdEyeClient client, String baseCollection)
      throws Exception {
    Assert.assertEquals(clientMap.get(collectionKey), client);
  }

  @Test(dataProvider = "collectionClientProvider")
  public void getByRequest(String collectionKey, ThirdEyeClient client, String baseCollection)
      throws Exception {
    ThirdEyeRequest mockRequest = mockRequest(collectionKey);
    Assert.assertEquals(clientMap.get(mockRequest), client);
  }

  @Test(dataProvider = "collectionClientProvider")
  public void execute(String collectionKey, ThirdEyeClient client, String baseCollection)
      throws Exception {
    ThirdEyeRequest request = mockRequest(collectionKey);
    clientMap.execute(request);
    verify(client).execute(baseRequest(baseCollection));
  }

  @Test(dataProvider = "collectionClientProvider")
  public void getRawResponse(String collectionKey, ThirdEyeClient client, String baseCollection)
      throws Exception {
    ThirdEyeRequest request = mockRequest(collectionKey);
    clientMap.getRawResponse(request);
    verify(client).getRawResponse(baseRequest(baseCollection));
  }

  @Test(dataProvider = "collectionClientProvider")
  public void getStarTreeConfig(String collectionKey, ThirdEyeClient client, String baseCollection)
      throws Exception {
    clientMap.getStarTreeConfig(collectionKey);
    verify(client).getStarTreeConfig(baseCollection);
  }

  @Test
  public void getCollections() throws Exception {
    initMap();
    clientMap.getCollections();
    for (ThirdEyeClient client : clients) {
      verify(client).getCollections();
    }
  }

  @Test(dataProvider = "collectionClientProvider")
  public void getSegmentDescriptors(String collectionKey, ThirdEyeClient client,
      String baseCollection) throws Exception {
    clientMap.getSegmentDescriptors(collectionKey);
    verify(client).getSegmentDescriptors(baseCollection);
  }

  @Test(dataProvider = "collectionClientProvider")
  public void getExpectedTimeBuckets(String collectionKey, ThirdEyeClient client,
      String baseCollection) throws Exception {
    ThirdEyeRequest request = mockRequest(collectionKey);
    clientMap.getExpectedTimeBuckets(request);
    verify(client).getExpectedTimeBuckets(baseRequest(baseCollection));
  }

  @Test
  public void clear() throws Exception {
    initMap();
    clientMap.clear();
    for (ThirdEyeClient client : clients) {
      verify(client).clear();
      // clients should be closed before disappearing from client map
      verify(client).close();
    }
    Assert.assertTrue(clientMap.getClients().isEmpty());
  }

  @Test
  public void close() throws Exception {
    initMap();
    clientMap.close();
    for (ThirdEyeClient client : clients) {
      verify(client).close();
    }
    // clients should still exist, just in closed state.
    Assert.assertFalse(clientMap.getClients().isEmpty());
  }

  @Test
  public void failSafeClientInit() throws Exception {
    // Two cases: config that fails to build, and config that produces faulty client
    ThirdEyeClientConfig badConfig = mock(ThirdEyeClientConfig.class);
    when(badConfig.buildClient())
        .thenThrow(new RuntimeException("Intentionally thrown failure from config buildClient"));

    ThirdEyeClient badClient = mock(ThirdEyeClient.class);
    ThirdEyeClientConfig badClientConfig = mock(ThirdEyeClientConfig.class);
    when(badClientConfig.buildClient()).thenReturn(badClient);
    when(badClient.getCollections())
        .thenThrow(new RuntimeException("Intentionally thrown failure from client getCollections"));

    List<ThirdEyeClientConfig> badClientConfigs = Arrays.asList(badConfig, badClientConfig);
    CollectionMapThirdEyeClient collectionMapThirdEyeClient =
        new CollectionMapThirdEyeClient(badClientConfigs);
    Assert.assertEquals(collectionMapThirdEyeClient.getClientConfigs(), badClientConfigs);
    // no collections should be retrieved
    Assert.assertEquals(collectionMapThirdEyeClient.getCollections(), Collections.emptyList());
    // badClient should have been 'built'
    Assert.assertEquals(collectionMapThirdEyeClient.getClients(), Arrays.asList(badClient));
  }

  @Test(dataProvider = "invalidCollectionKeyProvider", expectedExceptions = IllegalArgumentException.class)
  public void invalidCollectionKey(String invalidCollectionKey) {
    new CollectionKey(invalidCollectionKey);
  }

  @Test(dataProvider = "invalidCollectionPartsProvider", expectedExceptions = IllegalArgumentException.class)
  public void invalidCollectionParts(String prefix, String collection) {
    new CollectionKey(prefix, collection);
  }

  @DataProvider(name = "collectionClientProvider")
  public Object[][] collectionClientProvider() throws Exception {
    initMap();
    ArrayList<Object[]> entries = new ArrayList<Object[]>();
    for (ThirdEyeClient client : clients) {
      for (String collection : client.getCollections()) {
        String collectionKey = prefixMap.get(client)
            + CollectionMapThirdEyeClient.CollectionKey.SEPARATOR + collection;
        entries.add(new Object[] {
            collectionKey, client, collection
        });
      }
    }
    return entries.toArray(new Object[entries.size()][]);
  }

  @DataProvider(name = "invalidCollectionKeyProvider")
  public Object[][] invalidCollectionKeyProvider() throws Exception {
    List<String> invalidKeys =
        Arrays.asList(null, CollectionKey.SEPARATOR + CollectionKey.SEPARATOR);
    ArrayList<Object[]> entries = new ArrayList<Object[]>();
    for (String invalidKey : invalidKeys) {
      entries.add(new Object[] {
          invalidKey
      });
    }
    return entries.toArray(new Object[entries.size()][]);
  }

  @DataProvider(name = "invalidCollectionPartsProvider")
  public Object[][] invalidCollectionPartsProvider() throws Exception {
    String validPart = "test";
    List<Object[]> entries = Arrays.asList(new Object[] {
        null, validPart
    }, new Object[] {
        CollectionKey.SEPARATOR, validPart
    }, new Object[] {
        validPart, null
    }, new Object[] {
        validPart, CollectionKey.SEPARATOR
    });

    return entries.toArray(new Object[entries.size()][]);

  }

  private ThirdEyeRequest mockRequest(String collection) {
    ThirdEyeRequest request = mock(ThirdEyeRequest.class, "mockRequest_" + collection);
    when(request.getCollection()).thenReturn(collection);
    when(request.getDimensionValues()).thenReturn(LinkedListMultimap.<String, String> create());
    when(request.getGroupBy()).thenReturn(Collections.<String> emptySet());
    return request;
  }

  /**
   * The request received by the client should be identical to the original request, except with the
   * base collection rather than the full key.
   */
  private ThirdEyeRequest baseRequest(final String baseCollection) {
    return argThat(new ArgumentMatcher<ThirdEyeRequest>() {
      @Override
      public boolean matches(Object obj) {
        if (!(obj instanceof ThirdEyeRequest)) {
          return false;
        }
        ThirdEyeRequest request = (ThirdEyeRequest) obj;
        boolean equals = baseCollection.equals(request.getCollection());
        return equals;
      }
    });
  }

}
