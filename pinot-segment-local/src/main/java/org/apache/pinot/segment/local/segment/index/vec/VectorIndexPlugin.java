package org.apache.pinot.segment.local.segment.index.vec;

import com.google.auto.service.AutoService;
import org.apache.pinot.segment.spi.index.IndexPlugin;

@AutoService(IndexPlugin.class)
public class VectorIndexPlugin implements IndexPlugin<VectorIndexType> {

  private static final VectorIndexType INSTANCE = new VectorIndexType();

  @Override
  public VectorIndexType getIndexType() {
    return INSTANCE;
  }
}