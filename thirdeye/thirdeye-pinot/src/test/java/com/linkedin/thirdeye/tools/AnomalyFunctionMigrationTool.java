package com.linkedin.thirdeye.tools;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.anomalydetection.utils.StringUtil;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.util.DaoProviderUtil;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import javax.ws.rs.Produces;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AnomalyFunctionMigrationTool extends BaseThirdEyeTool {
  private static final Logger LOG = LoggerFactory.getLogger(AnomalyFunctionMigrationTool.class);
  private static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  public static String PATH_TO_ANOMALY_FUNCTION_PROPERTIES = "detector-config/anomaly-functions/functions.properties";
  public static String PATH_TO_PERSISTENCE = "persistence.yml";

  private Map<String, String> propertyKeyMap;
  private AnomalyFunctionFactory anomalyFunctionFactory;
  private AnomalyFunctionManager anomalyFunctionDAO;

  public AnomalyFunctionMigrationTool(String configPath) {
    File configFolder = new File(configPath);
    setAnomalyFunctionFactory((new File(configFolder, PATH_TO_ANOMALY_FUNCTION_PROPERTIES)).getAbsolutePath());
    init((new File(configFolder, PATH_TO_PERSISTENCE)));
    anomalyFunctionDAO = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.AnomalyFunctionManagerImpl.class);
    this.propertyKeyMap = Collections.EMPTY_MAP;
  }

  public void setAnomalyFunctionFactory(String configPaht) {
    anomalyFunctionFactory = new AnomalyFunctionFactory(configPaht);
  }

  public void setPropertyKeyMap(String json) throws IOException {
    setPropertyKeyMap(OBJECT_MAPPER.readValue(json, HashMap.class));
  }

  public void setPropertyKeyMap(Map<String, String> map) {
    this.propertyKeyMap = map;
  }

  public Map<String, String> getPropertyKeyMap() {
    return this.propertyKeyMap;
  }

  public void migrateTo(String srcFunctionType, String destFunctionType) {
    List<AnomalyFunctionDTO> anomalyFunctions = new ArrayList<>();
    for (AnomalyFunctionDTO anomalyFunction : anomalyFunctionDAO.findAll()) {
      if (anomalyFunction.getType().equalsIgnoreCase(srcFunctionType)) {
        anomalyFunctions.add(anomalyFunction);
      }
    }
    migrate(anomalyFunctions, destFunctionType);
  }

  public void migrateTo(List<Long> functionIds, String destFunctionType) {
    List<AnomalyFunctionDTO> anomalyFunctions = new ArrayList<>();
    for (long functionId : functionIds) {
      AnomalyFunctionDTO anomalyFunction = anomalyFunctionDAO.findById(functionId);
      if (anomalyFunction != null) {
        anomalyFunctions.add(anomalyFunction);
      }
    }
    migrate(anomalyFunctions, destFunctionType);
  }

  private void migrate(List<AnomalyFunctionDTO> anomalyFunctions, String destFunctionType) {
    for (AnomalyFunctionDTO anomalyFunction : anomalyFunctions) {
      migrate(anomalyFunction, destFunctionType);
    }
  }

  private void migrate(AnomalyFunctionDTO anomalyFunction, String destFunctionType) {
    String originalType = anomalyFunction.getType();
    anomalyFunction.setType(destFunctionType);
    if (isValid(anomalyFunction)) {
      updateProperties(anomalyFunction);
      anomalyFunctionDAO.update(anomalyFunction);
    } else {
      anomalyFunction.setType(originalType);
    }
  }

  private boolean isValid(AnomalyFunctionDTO anomalyFunction) {
    try {
      anomalyFunctionFactory.fromSpec(anomalyFunction);
    } catch (Exception e) {
      return false;
    }
    return true;
  }

  private void updateProperties(AnomalyFunctionDTO anomalyFunction) {
    Properties oldProperties = anomalyFunction.toProperties();
    Properties newProperties = new Properties();
    for (String propertyName : oldProperties.stringPropertyNames()) {
      if (propertyKeyMap.containsKey(propertyName)) {
        newProperties.setProperty(propertyKeyMap.get(propertyName), oldProperties.getProperty(propertyName));
      }
    }
    anomalyFunction.setProperties(StringUtil.encodeCompactedProperties(newProperties));
  }

  public static void main(String[] args){
    if (args.length != 3) {
      LOG.warn("Invalid number of parameters.\nShould be in format of <config_path> <source_function_type>"
          + " <destination_function_type>");
    }

    String configPath = args[0];
    String srcFunctionType = args[1];
    String destFunctionType = args[2];

    AnomalyFunctionMigrationTool tool = new AnomalyFunctionMigrationTool(configPath);
  }
}
