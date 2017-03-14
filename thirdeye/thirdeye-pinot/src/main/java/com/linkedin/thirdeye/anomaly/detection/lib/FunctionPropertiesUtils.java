package com.linkedin.thirdeye.anomaly.detection.lib;

import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import java.util.Map;
import java.util.Properties;


/**
 * This utility class is used to apply tuning configuration to function properties, and convert properties to String
 */
public class FunctionPropertiesUtils {
  /**
   * Convert Properties to String following the format in TE
   * @param props
   * @return a String of given Properties
   */
  public static String propertiesToString(Properties props){
    StringBuilder stringBuilder = new StringBuilder();
    for(Map.Entry entry : props.entrySet()){
      stringBuilder.append(entry.getKey() + "=" + entry.getValue() + ";");
    }
    stringBuilder.deleteCharAt(stringBuilder.length() - 1);
    return stringBuilder.toString();
  }

  public static void applyConfigurationToProperties(AnomalyFunctionDTO functionDTO, Map<String, String> config) {
    Properties properties = functionDTO.toProperties();
    for(Map.Entry<String, String> entry : config.entrySet()) {
      properties.setProperty(entry.getKey(), entry.getValue());
    }
    functionDTO.setProperties(propertiesToString(properties));
  }
}
