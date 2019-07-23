package org.apache.pinot.tools.tuner.meta.manager;

public interface MetaManager extends MetaDataProperties {
  /**
   * extract from some source(file/API) and cache metadata in JsonNode
   * @return
   */
  MetaManager cache();
}
