package org.apache.pinot.tools;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.core.util.trace.ContinuousJfrStarter;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.spi.utils.CommonConstants;


public class JfrQuickstart extends Quickstart {
  @Override
  public List<String> types() {
    return Collections.singletonList("JFR");
  }

  @Override
  protected Map<String, String> getClusterConfigOverrides() {
    Map<String, String> clusterConfigOverrides = new HashMap<>(super.getClusterConfigOverrides());
    String jfrDirectory = System.getProperty("user.dir") + "/jfr";
    clusterConfigOverrides.put(CommonConstants.JFR + ".enabled", "true");
    clusterConfigOverrides.put(CommonConstants.JFR + "." + ContinuousJfrStarter.DIRECTORY, jfrDirectory);
    clusterConfigOverrides.put(CommonConstants.JFR + ".dumpPath", jfrDirectory);
    clusterConfigOverrides.put(CommonConstants.JFR + ".preserveRepository", "true");
    return clusterConfigOverrides;
  }

  public static void main(String[] args)
      throws Exception {
    PluginManager.get().init();
    new JfrQuickstart().execute();
  }
}
