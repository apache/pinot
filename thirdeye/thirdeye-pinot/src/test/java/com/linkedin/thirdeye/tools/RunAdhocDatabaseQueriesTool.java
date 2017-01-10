package com.linkedin.thirdeye.tools;

import com.linkedin.thirdeye.anomaly.override.OverrideConfigHelper;
import com.linkedin.thirdeye.datalayer.dto.OverrideConfigDTO;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import java.util.Map;

import com.linkedin.thirdeye.detector.metric.transfer.ScalingFactor;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.EmailConfigurationDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;

/**
 * Run adhoc queries to db
 */
public class RunAdhocDatabaseQueriesTool extends GenericDatabaseAccessTool {

  private static final Logger LOG = LoggerFactory.getLogger(RunAdhocDatabaseQueriesTool.class);

  public RunAdhocDatabaseQueriesTool(File persistenceFile)
      throws Exception {
    super(persistenceFile);
  }

  private void updateFields() {
    List<EmailConfigurationDTO> emailConfigs = emailConfigurationDAO.findAll();
    for (EmailConfigurationDTO emailConfig : emailConfigs) {
      LOG.info(emailConfig.getId() + " " + emailConfig.getToAddresses());
    }
  }

  private void updateField(Long id) {
    AnomalyFunctionDTO anomalyFunction = anomalyFunctionDAO.findById(id);
    //anomalyFunction.setCron("0/10 * * * * ?");
    anomalyFunction.setActive(true);
    anomalyFunctionDAO.update(anomalyFunction);
  }

  private void customFunction() {
    List<AnomalyFunctionDTO> anomalyFunctionDTOs = anomalyFunctionDAO.findAll();
    for (AnomalyFunctionDTO anomalyFunctionDTO : anomalyFunctionDTOs) {
      anomalyFunctionDTO.setActive(false);
      anomalyFunctionDAO.update(anomalyFunctionDTO);
    }
  }

  private void updateNotified() {
    List<MergedAnomalyResultDTO> mergedResults = mergedResultDAO.findAll();
    for (MergedAnomalyResultDTO mergedResult : mergedResults) {
      mergedResult.setNotified(true);
      mergedResultDAO.update(mergedResult);
    }
  }

  // util function for setting Metric scaling factor

  private static Map<String, List<String>> getTargetLevelSet(String commaSeparatedMetricNames) {
    Map<String, List<String>> targetLevelSet1 = new HashMap<>();
    targetLevelSet1.put(OverrideConfigHelper.TARGET_METRIC,
        new ArrayList<>(Arrays.asList(commaSeparatedMetricNames.split(","))));
    return(targetLevelSet1);
  }

  private static List<Map<String, String>> getOverrideScalingFactorSet (double[] scalingFactors) {
    List<Map<String, String>> overridePropertySet1 = new ArrayList<>();
    // fill in the scaling factors
    for(double scaleFactor : scalingFactors) {
      Map<String, String> overrideProperty = new HashMap<>();
      overrideProperty.put(ScalingFactor.SCALING_FACTOR, String.valueOf(scaleFactor));
      overridePropertySet1.add(overrideProperty);
    }
    return(overridePropertySet1);
  }


  public static void main(String[] args) throws Exception {

    File persistenceFile = new File(args[0]);
    if (!persistenceFile.exists()) {
      System.err.println("Missing file:" + persistenceFile);
      System.exit(1);
    }
    RunAdhocDatabaseQueriesTool dq = new RunAdhocDatabaseQueriesTool(persistenceFile);
    List<Map<String, List<String>>> targetLevelList = new ArrayList<>();
    List<List<Map<String, String>>> overridePropertyList = new ArrayList<>();


//    List<Integer> idList = Arrays
//        .asList(954637, 954638, 954639, 954640, 954641, 954642, 954643, 954644, 954645, 954646,
//            954647, 954648);
//    int idIdx = 0;

    // Override config for 12/21 - 12/27
    // Override Config for SU_Request Desktop
    targetLevelList.add(getTargetLevelSet("SU_Request_from_Desktop"));
    overridePropertyList.add(getOverrideScalingFactorSet(new double[]{1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0}));

    // Override Config for SU_Request Mobile
    targetLevelList.add(getTargetLevelSet("SU_Request_from_Android,SU_Request_from_iOS"));
    overridePropertyList.add(getOverrideScalingFactorSet(new double[]{1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0}));

    // Override Config for Imp Desktop
    targetLevelList.add(getTargetLevelSet("SU_Impression_Count_on_Desktop"));
    overridePropertyList.add(getOverrideScalingFactorSet(new double[]{1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0}));

    // Override Config for Imp Mobile
    targetLevelList.add(getTargetLevelSet("SU_Impression_Count_on_Android,SU_Impression_Count_on_iOS"));
    overridePropertyList.add(getOverrideScalingFactorSet(new double[]{1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0}));

    // Override Config for Clk Desktop
    targetLevelList.add(getTargetLevelSet("SU_Click_Count_on_Desktop"));
    overridePropertyList.add(getOverrideScalingFactorSet(new double[]{1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0}));

    // Override Config for Clk Mobile
    targetLevelList.add(getTargetLevelSet("SU_Click_Count_on_Android,SU_Click_Count_on_iOS"));
    overridePropertyList.add(getOverrideScalingFactorSet(new double[]{1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0}));

    for (int i = 0; i < targetLevelList.size(); ++i) {
      DateTime time = new DateTime(2014, 12, 21, 0, 0);
      Map<String, List<String>> targetLevel = targetLevelList.get(i);
      List<Map<String, String>> overrideProperties = overridePropertyList.get(i);

      for (int day = 0; day < 6; ++day) {
        OverrideConfigDTO overrideConfigDTO = new OverrideConfigDTO();
        overrideConfigDTO.setStartTime(time.getMillis());
        time = time.plusDays(1);
        overrideConfigDTO.setEndTime(time.getMillis());
        overrideConfigDTO.setTargetEntity(OverrideConfigHelper.ENTITY_TIME_SERIES);
        overrideConfigDTO.setTargetLevel(targetLevel);
        overrideConfigDTO.setOverrideProperties(overrideProperties.get(day));
        overrideConfigDTO.setActive(true);

        Long id = dq.createOverrideConfig(overrideConfigDTO);
        LOG.info("Inserted config {}", id);
//        dq.updateOverrideConfig(idList.get(idIdx++), overrideConfigDTO);
      }
    }
  }

}
