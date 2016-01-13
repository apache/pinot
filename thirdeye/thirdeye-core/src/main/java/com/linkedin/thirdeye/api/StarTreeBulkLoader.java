package com.linkedin.thirdeye.api;

import java.io.File;
import java.io.IOException;

public interface StarTreeBulkLoader {
  void bulkLoad(StarTree starTree, File rootDir, File tmpDir) throws IOException;
}
