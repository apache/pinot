package com.linkedin.thirdeye.tools;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class TestCloneAnomalyFunctionTool {
  private static final Logger LOG = LoggerFactory.getLogger(TestCloneAnomalyFunctionTool.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());

  public static void main(String[] args) throws Exception {

    if (args.length != 1) {
      System.err.println("USAGE TestCloneAnomalyFunctionTool <config_yml_file>\n "
          + "Please provide the configure file");
      System.exit(1);
    }

    File configFile = new File(args[0]);
    CloneAnomalyFunctionToolConfig config =
        OBJECT_MAPPER.readValue(configFile, CloneAnomalyFunctionToolConfig.class);

    File persistenceFile = new File(config.getPersistenceFile());
    if (!persistenceFile.exists()) {
      System.err.println("Missing file:" + persistenceFile);
      System.exit(1);
    }

    String functionIds = config.getFunctionIds();
    String nameTags = config.getCloneNameTags();
    LOG.debug("function ids: {}", functionIds);
    LOG.debug("name tags: {}", nameTags);
    boolean doCloneAnomalyResults = false;
    String cloneAnomalyResults = config.getDoCloneAnomalyResults();
    if (StringUtils.isNotBlank(cloneAnomalyResults)) {
      doCloneAnomalyResults = Boolean.parseBoolean(cloneAnomalyResults);
    }

    CloneAnomalyFunctionTool tool = new CloneAnomalyFunctionTool(persistenceFile, functionIds, nameTags);
    int len = tool.getFunctionIds().size();
    for (int i = 0; i < len; i++) {
      long id = tool.getFunctionIds().get(i);
      String tag = tool.getCloneNameTags().get(i);
      LOG.debug("id: {}, tag: {}", id, tag);
      tool.cloneAnomalyFunctionById(id, tag, doCloneAnomalyResults);
    }

    System.exit(0);
  }

}
