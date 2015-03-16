package com.linkedin.pinot.common.utils;

import java.util.Arrays;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyPathConfig;
import org.apache.helix.PropertyType;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.store.zk.ZkHelixPropertyStore;


/**
 * TODO Document me!
 *
 * @author jfim
 */
public class ZkUtils {
  public static ZkHelixPropertyStore<ZNRecord> getZkPropertyStore(HelixManager helixManager, String clusterName) {
    ZkBaseDataAccessor<ZNRecord> baseAccessor =
        (ZkBaseDataAccessor<ZNRecord>) helixManager.getHelixDataAccessor().getBaseDataAccessor();
    String propertyStorePath = PropertyPathConfig.getPath(PropertyType.PROPERTYSTORE, clusterName);

    ZkHelixPropertyStore<ZNRecord> propertyStore =
        new ZkHelixPropertyStore<ZNRecord>(baseAccessor, propertyStorePath, Arrays.asList(propertyStorePath));

    return propertyStore;
  }
}
