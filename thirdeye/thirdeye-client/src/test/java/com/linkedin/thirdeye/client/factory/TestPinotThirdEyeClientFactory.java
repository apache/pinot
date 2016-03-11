package com.linkedin.thirdeye.client.factory;

import static com.linkedin.thirdeye.client.PinotThirdEyeClient.BROKERS_PROPERTY_KEY;
import static com.linkedin.thirdeye.client.PinotThirdEyeClient.CLUSTER_NAME_PROPERTY_KEY;
import static com.linkedin.thirdeye.client.PinotThirdEyeClient.CONTROLLER_HOST_PROPERTY_KEY;
import static com.linkedin.thirdeye.client.PinotThirdEyeClient.CONTROLLER_PORT_PROPERTY_KEY;
import static com.linkedin.thirdeye.client.PinotThirdEyeClient.FIXED_COLLECTIONS_PROPERTY_KEY;
import static com.linkedin.thirdeye.client.PinotThirdEyeClient.TAG_PROPERTY_KEY;
import static com.linkedin.thirdeye.client.factory.PinotThirdEyeClientFactory.ZK_URL_PROPERTY_KEY;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.linkedin.thirdeye.client.PinotThirdEyeClient;
import com.linkedin.thirdeye.client.ThirdEyeClient;

public class TestPinotThirdEyeClientFactory {
  private static final String DEFAULT_HOST = "localhost";
  private static final String DEFAULT_PORT = "0";
  private static final String DEFAULT_BROKERS = "localhost:1,localhost:2";
  private static final String DEFAULT_ZK_URL = "localhost:1";
  private static final String DEFAULT_CLUSTER_NAME = "pinotCluster";
  private static final String DEFAULT_TAG = "thirdeyeBroker";
  private static final String DEFAULT_FIXED_COLLECTIONS = "collection1,collection2";
  private PinotThirdEyeClientFactory factory;

  @BeforeMethod
  public void setup() {
    factory = new PinotThirdEyeClientFactory();
    factory.configureCache(null);
  }

  @Test(dataProvider = "validProps")
  public void getRawClient(Properties props) throws Exception {
    ThirdEyeClient client = factory.getRawClient(props);
    Assert.assertTrue(client instanceof PinotThirdEyeClient);
    if (props.containsKey(FIXED_COLLECTIONS_PROPERTY_KEY)) {
      String expectedCollections = props.getProperty(FIXED_COLLECTIONS_PROPERTY_KEY);
      List<String> collections = client.getCollections();
      Assert.assertEquals(collections, Arrays.asList(expectedCollections.split(",")));
    }
  }

  @Test(dataProvider = "invalidProps", expectedExceptions = IllegalArgumentException.class)
  public void getRawClientErrors(Properties props) throws Exception {
    ThirdEyeClient client = factory.getRawClient(props);
  }

  @DataProvider(name = "validProps")
  public Object[][] validProps() {
    List<Properties> propList = new LinkedList<>();
    propList.add(createProps(DEFAULT_HOST, DEFAULT_PORT, DEFAULT_BROKERS));
    propList
        .add(createProps(DEFAULT_HOST, DEFAULT_PORT, DEFAULT_BROKERS, DEFAULT_FIXED_COLLECTIONS));
    // these take too long and fail because they try to use the zk instance.
    // propList.add(
    // createProps(DEFAULT_HOST, DEFAULT_PORT, DEFAULT_ZK_URL, DEFAULT_CLUSTER_NAME, DEFAULT_TAG));
    // propList.add(createProps(DEFAULT_HOST, DEFAULT_PORT, DEFAULT_ZK_URL, DEFAULT_CLUSTER_NAME,
    // DEFAULT_TAG, DEFAULT_FIXED_COLLECTIONS));
    return convertToObjectArr2(propList);
  }

  @DataProvider(name = "invalidProps")
  public Object[][] invalidProps() {
    List<Properties> propList = new LinkedList<>();
    propList.add(createProps(null, DEFAULT_PORT, DEFAULT_BROKERS));
    propList.add(createProps(DEFAULT_HOST, null, DEFAULT_BROKERS));
    propList.add(createProps(DEFAULT_HOST, DEFAULT_PORT, null));
    propList.add(createProps(DEFAULT_HOST, DEFAULT_PORT, null, DEFAULT_CLUSTER_NAME, DEFAULT_TAG));
    propList.add(createProps(DEFAULT_HOST, DEFAULT_PORT, DEFAULT_ZK_URL, null, DEFAULT_TAG));
    propList
        .add(createProps(DEFAULT_HOST, DEFAULT_PORT, DEFAULT_ZK_URL, DEFAULT_CLUSTER_NAME, null));

    return convertToObjectArr2(propList);
  }

  private Properties createProps(String host, String port, String brokers) {
    return createProps(host, port, brokers, null);
  }

  private Properties createProps(String host, String port, String brokers,
      String fixedCollections) {
    Properties props = createHostProps(host, port);
    addIfNotNull(props, BROKERS_PROPERTY_KEY, brokers);
    addIfNotNull(props, FIXED_COLLECTIONS_PROPERTY_KEY, fixedCollections);
    return props;
  }

  private Properties createProps(String host, String port, String zkUrl, String clusterName,
      String tag) {
    return createProps(host, port, zkUrl, clusterName, tag, null);
  }

  private Properties createProps(String host, String port, String zkUrl, String clusterName,
      String tag, String fixedCollections) {
    Properties props = createHostProps(host, port);
    addIfNotNull(props, ZK_URL_PROPERTY_KEY, zkUrl);
    addIfNotNull(props, CLUSTER_NAME_PROPERTY_KEY, clusterName);
    addIfNotNull(props, TAG_PROPERTY_KEY, tag);
    addIfNotNull(props, FIXED_COLLECTIONS_PROPERTY_KEY, fixedCollections);
    return props;
  }

  private Properties createHostProps(String host, String port) {
    Properties props = new Properties();
    addIfNotNull(props, CONTROLLER_HOST_PROPERTY_KEY, host);
    addIfNotNull(props, CONTROLLER_PORT_PROPERTY_KEY, port);
    return props;
  }

  private void addIfNotNull(Properties props, String key, String value) {
    if (value != null) {
      props.put(key, value);
    }
  }

  private Object[][] convertToObjectArr2(List<Properties> propList) {
    List<Object[]> propsWrapperList = new LinkedList<>();
    for (Properties props : propList) {
      propsWrapperList.add(new Object[] {
          props
      });
    }
    return propsWrapperList.toArray(new Object[propsWrapperList.size()][]);
  }
}
