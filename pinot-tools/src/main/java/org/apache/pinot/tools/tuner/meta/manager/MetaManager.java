package org.apache.pinot.tools.tuner.meta.manager;

public interface MetaManager extends MetaDataProperties {
  MetaManager cache(); //extract from source and cache metadata in JsonNode
}
