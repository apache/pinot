package com.linkedin.thirdeye.tools;

import com.linkedin.thirdeye.datalayer.bao.EmailConfigurationManager;
import com.linkedin.thirdeye.datalayer.dto.EmailConfigurationDTO;
import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.linkedin.thirdeye.common.persistence.PersistenceUtil;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.WebappConfigManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.WebappConfigDTO;
import com.linkedin.thirdeye.datalayer.util.DaoProviderUtil;

public class Hibernate2JDBCMigration {

  private File sourceConfigFile;
  private File targetConfigFile;

  public Hibernate2JDBCMigration(File sourceConfigFile, File targetConfigFile) {
    this.sourceConfigFile = sourceConfigFile;
    this.targetConfigFile = targetConfigFile;
  }

  public void run() throws Exception {
    initSource();
    initTarget();
    cloneWebappConfig();
    cloneAnomalyFunctions();
    cloneEmailConfigs();
  }

  private void cloneEmailConfigs() {
    EmailConfigurationManager hibernateManager = PersistenceUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.hibernate.EmailConfigurationManagerImpl.class);
    EmailConfigurationManager jdbcManager = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.EmailConfigurationManagerImpl.class);
    List<EmailConfigurationDTO> dtoSrcList = hibernateManager.findAll();
    List<EmailConfigurationDTO> dtoTgtList = jdbcManager.findAll();
    Set<String> uniques = new HashSet<>();
    //ALTER TABLE `webapp_config_index` ADD UNIQUE `unique_index`(`name`, `collection`, `type`);
    for (EmailConfigurationDTO dto : dtoTgtList) {
      String key = dto.getCollection() + "_" + dto.getMetric();
      uniques.add(key);
    }
    for (EmailConfigurationDTO dto : dtoSrcList) {
      String key = dto.getCollection() + "_" + dto.getMetric();
      if (!uniques.contains(key)) {
        dto.setId(null);
        jdbcManager.save(dto);
      } else {
        System.out.println(
            "Skipping creating emailConfig for key:" + key + " since it already exists.");
      }
    }
  }

  private void cloneAnomalyFunctions() {
    AnomalyFunctionManager hibernateManager = PersistenceUtil.getInstance(
        com.linkedin.thirdeye.datalayer.bao.hibernate.AnomalyFunctionManagerImpl.class);
    AnomalyFunctionManager jdbcManager = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.AnomalyFunctionManagerImpl.class);
    List<AnomalyFunctionDTO> dtoSrcList = hibernateManager.findAll();
    List<AnomalyFunctionDTO> dtoTgtList = jdbcManager.findAll();
    Set<String> uniques = new HashSet<>();
    for (AnomalyFunctionDTO dto : dtoTgtList) {
      uniques.add(dto.getFunctionName());
    }
    for (AnomalyFunctionDTO dto : dtoSrcList) {
      if (!uniques.contains(dto.getFunctionName())) {
        dto.setId(null);
        jdbcManager.save(dto);
      } else {
        System.out.println(
            "Skipping creating function for key:" + dto.getFunctionName() + " since it already exists.");
      }
    }
  }

  private void cloneWebappConfig() {
    WebappConfigManager hibernateManager = PersistenceUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.hibernate.WebappConfigManagerImpl.class);
    WebappConfigManager jdbcManager = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.WebappConfigManagerImpl.class);
    List<WebappConfigDTO> dtoSrcList = hibernateManager.findAll();
    List<WebappConfigDTO> dtoTgtList = jdbcManager.findAll();
    Set<String> uniques = new HashSet<>();
    //ALTER TABLE `webapp_config_index` ADD UNIQUE `unique_index`(`name`, `collection`, `type`);
    for (WebappConfigDTO dto : dtoTgtList) {
      String key = dto.getName() + "_" + dto.getCollection() + "_" + dto.getType().toString();
      uniques.add(key);
    }
    for (WebappConfigDTO dto : dtoSrcList) {
      String key = dto.getName() + "_" + dto.getCollection() + "_" + dto.getType().toString();
      if (!uniques.contains(key)) {
        dto.setId(null);
        jdbcManager.save(dto);
      } else {
        System.out.println(
            "Skipping creating webappconfig for key:" + key + " since it already exists.");
      }
    }
  }

  public void initSource() throws Exception {
    PersistenceUtil.init(sourceConfigFile);
  }

  public void initTarget() throws Exception {
    DaoProviderUtil.init(targetConfigFile);
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("USAGE Hibernate2JDBCMigration <sourceConfigFile> <targetConfigFile>");
      System.exit(1);
    }
    File sourceConfigFile = new File(args[0]);
    File targetConfigFile = new File(args[1]);
    if (!sourceConfigFile.exists()) {
      System.err.println("Missing file:" + sourceConfigFile);
      System.exit(1);
    }
    if (!targetConfigFile.exists()) {
      System.err.println("Missing file:" + targetConfigFile);
      System.exit(1);
    }

    Hibernate2JDBCMigration migrationRunner =
        new Hibernate2JDBCMigration(sourceConfigFile, targetConfigFile);
    migrationRunner.run();
  }

}
