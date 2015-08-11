package com.linkedin.thirdeye.reporting.api;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map.Entry;

import org.quartz.CronScheduleBuilder;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ReportScheduler {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReportScheduler.class);

  private Scheduler quartzScheduler = null;
  private File reportConfigFolder;
  private String reportEmailTemplatePath;
  private String serverUri;
  private String dashboardUri;

  public ReportScheduler(File reportConfigFolder, String reportEmailTemplatePath, String serverUri, String dashboardUri) {
    this.reportConfigFolder = reportConfigFolder;
    this.reportEmailTemplatePath = reportEmailTemplatePath;
    this.serverUri = serverUri;
    this.dashboardUri = dashboardUri;
  }

  public void start() throws SchedulerException, FileNotFoundException, IOException {

    LOGGER.info("Starting report scheduler");
    quartzScheduler = StdSchedulerFactory.getDefaultScheduler();
    quartzScheduler.start();

    File[] reportConfigFiles = reportConfigFolder.listFiles(new FileFilter() {
      @Override
      public boolean accept(File pathname) {
        return pathname.getName().endsWith(ReportConstants.YAML_FILE_SUFFIX);
      }
    });

    try {
      for (File reportConfigFile : reportConfigFiles) {
        ReportConfig reportConfig = ReportConfig.decode(new FileInputStream(reportConfigFile));
        LOGGER.info("Config {}", reportConfig.encode());
        LOGGER.info("Job data: ( reportConfigFile:{}, serverUri:{}, dashboardUri:{}, templatePath:{}",
            reportConfigFile, serverUri, dashboardUri, reportEmailTemplatePath);

        JobDataMap newJobDataMap = new JobDataMap();
        newJobDataMap.put(ReportConstants.CONFIG_FILE_KEY, reportConfigFile.getPath());
        newJobDataMap.put(ReportConstants.SERVER_URI_KEY, serverUri);
        newJobDataMap.put(ReportConstants.DASHBOARD_URI_KEY, dashboardUri);
        newJobDataMap.put(ReportConstants.TEMPLATE_PATH_KEY, reportEmailTemplatePath);

        for (Entry<String, ScheduleSpec> entry :reportConfig.getSchedules().entrySet()) {
          JobDetail job = JobBuilder.newJob(ReportGenerator.class)
              .withDescription(entry.getKey())
              .usingJobData(newJobDataMap)
              .build();
          Trigger trigger = TriggerBuilder.newTrigger()
              .withDescription(entry.getKey())
              .withSchedule(CronScheduleBuilder.cronSchedule(entry.getValue().getCron()))
              .build();

          LOGGER.info("Scheduling job {} with trigger {}", job.getDescription(), entry.getValue().getCron());
          quartzScheduler.scheduleJob(job, trigger);
        }
      }
    } catch (Exception e) {
      LOGGER.error(e.toString());
    }
  }

  public Scheduler getQuartzScheduler() {
    return quartzScheduler;
  }

  public void setQuartzScheduler(Scheduler quartzScheduler) {
    this.quartzScheduler = quartzScheduler;
  }

  public void stop() throws SchedulerException {
    LOGGER.info("Shutting down report scheduler");
    quartzScheduler.shutdown();
  }

}
