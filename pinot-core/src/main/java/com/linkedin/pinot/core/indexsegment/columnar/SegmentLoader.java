package com.linkedin.pinot.core.indexsegment.columnar;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.commons.configuration.ConfigurationException;

import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.columnar.creator.V1Constants;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;


public class SegmentLoader {

  public enum IO_MODE {
    heap,
    mmap;
  }

  public static IndexSegment load(File indexDir, IO_MODE mode) throws ConfigurationException, IOException {
    switch (mode) {
      case heap:
        return loadHeap(indexDir);
      case mmap:
        return loadMmap(indexDir);
    }
    return null;
  }

  public static SegmentVersion extractVersion(File indexDir) throws IOException {
    File versions = new File(indexDir, V1Constants.VERSIONS_FILE);
    DataInputStream is = new DataInputStream(new FileInputStream(versions));
    byte[] vce = new byte[(int) versions.length()];
    is.read(vce, 0, vce.length);
    String v = new String(vce);
    return SegmentVersion.valueOf(v);
  }

  public static IndexSegment loadMmap(File indexDir) throws ConfigurationException, IOException {
    return new ColumnarSegment(indexDir, IO_MODE.mmap);
  }

  public static IndexSegment loadHeap(File indexDir) throws ConfigurationException, IOException {
    return new ColumnarSegment(indexDir, IO_MODE.heap);
  }
}
