package com.linkedin.thirdeye.tools;

import com.linkedin.thirdeye.datalayer.util.DaoProviderUtil;
import java.io.File;
import java.io.IOException;


public class BaseThirdEyeTool {
  public BaseThirdEyeTool() {
  }

  public void init(File persistenceFile) {
    DaoProviderUtil.init(persistenceFile);
  }

  public void init(String persistencePath) throws IOException {
    File persistenceFile = new File(persistencePath);
    if (!persistenceFile.exists() || persistenceFile.canRead()) {
      throw new IOException("File doesn't exist or not readable");
    }
    init(new File(persistencePath));
  }
}
