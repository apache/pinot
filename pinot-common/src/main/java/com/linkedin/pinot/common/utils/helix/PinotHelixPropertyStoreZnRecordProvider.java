package com.linkedin.pinot.common.utils.helix;

import org.apache.helix.AccessOption;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;


public class PinotHelixPropertyStoreZnRecordProvider {

  private final ZkHelixPropertyStore<ZNRecord> propertyStore;
  private final String pathPrefix;

  private PinotHelixPropertyStoreZnRecordProvider() {
    this.pathPrefix = null;
    this.propertyStore = null;
  }

  private PinotHelixPropertyStoreZnRecordProvider(ZkHelixPropertyStore<ZNRecord> propertyStore, String relativePathName) {
    this.propertyStore = propertyStore;
    this.pathPrefix = relativePathName;
  }

  public static PinotHelixPropertyStoreZnRecordProvider forSchema(ZkHelixPropertyStore<ZNRecord> propertyStore) {
    return new PinotHelixPropertyStoreZnRecordProvider(propertyStore, "/SCHEMAS");
  }

  public static PinotHelixPropertyStoreZnRecordProvider forTable(ZkHelixPropertyStore<ZNRecord> propertyStore) {
    return new PinotHelixPropertyStoreZnRecordProvider(propertyStore, "/CONFIGS/TABLES");
  }

  public static PinotHelixPropertyStoreZnRecordProvider forSegments(ZkHelixPropertyStore<ZNRecord> propertyStore) {
    return new PinotHelixPropertyStoreZnRecordProvider(propertyStore, "/SEGMENTS");
  }

  public ZNRecord get(String name) {
    return propertyStore.get(pathPrefix + "/" + name, null, AccessOption.PERSISTENT);
  }

  public void set(String name, ZNRecord record) {
    propertyStore.set(pathPrefix + "/" + name, record, AccessOption.PERSISTENT);
  }

  public boolean exist(String path) {
    return propertyStore.exists(pathPrefix + "/" + path, AccessOption.PERSISTENT);
  }

  public String getRelativePath() {
    return pathPrefix;
  }
}
