package org.apache.pinot.tools.tuner.meta.manager;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.math.fraction.BigFraction;


public interface MetaManager extends MetaDataProperties{

  MetaManager cache(); //extract from source and cache metadata in JsonNode
}
