package org.apache.pinot.tools.tuner.meta.manager;

public interface MetaManager extends MetaDataProperties {
  MetaManager cache(); //extract from some source and cache metadata in JsonNode
}
