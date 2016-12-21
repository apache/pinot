package com.linkedin.thirdeye.tools.anomaly.report;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.linkedin.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import io.dropwizard.configuration.ConfigurationFactory;
import io.dropwizard.jackson.Jackson;
import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.validation.Validation;
import org.quartz.CronExpression;

public class AnomalyReportDriver {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());
  private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  public static void main(String[] args) throws Exception {

    Runnable job = new Runnable() {
      @Override public void run() {
        try {
          System.out.println("Running report generation");
          runReportGenerator();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    };

    ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    long delaySeconds = (getNextExecutionTime().getTime() - System.currentTimeMillis()) / 1000;

    // Repeat every 4 hours
   // scheduler.scheduleAtFixedRate(job, delaySeconds, TimeUnit.HOURS.toSeconds(4), TimeUnit.SECONDS);

    // Repeat every Day
    scheduler.scheduleAtFixedRate(job, delaySeconds, TimeUnit.DAYS.toSeconds(1), TimeUnit.SECONDS);

    System.out.println("Press q to quit");
    int ch = System.in.read();
    while (ch != 'q') {
      System.out.println("Press q to quit, next execution time : " + getNextExecutionTime());
      ch = System.in.read();
    }
    System.exit(-1);
  }

  static Date getNextExecutionTime() throws Exception {
    CronExpression cronExpression = new CronExpression("0 0 7 * * ?");
    return cronExpression.getNextValidTimeAfter(new Date());
  }

  static void runReportGenerator() throws Exception {

    File configFile = new File(
        "/opt/Code/pinot2_0/thirdeye/thirdeye-pinot/src/test/resources/custom-anomaly-report-config.yml");

    AnomalyReportConfig config = OBJECT_MAPPER.readValue(configFile, AnomalyReportConfig.class);

    File persistenceFile = new File(config.getThirdEyeConfigDirectoryPath() + "/persistence.yml");
    if (!persistenceFile.exists()) {
      System.err.println("Missing file:" + persistenceFile);
      System.exit(1);
    }

    File detectorConfigFile = new File(config.getThirdEyeConfigDirectoryPath() + "/detector.yml");
    if (!detectorConfigFile.exists()) {
      System.err.println("Missing file:" + detectorConfigFile);
      System.exit(1);
    }

    ConfigurationFactory<ThirdEyeAnomalyConfiguration> factory =
        new ConfigurationFactory<>(ThirdEyeAnomalyConfiguration.class,
            Validation.buildDefaultValidatorFactory().getValidator(), Jackson.newObjectMapper(),
            "");
    ThirdEyeAnomalyConfiguration detectorConfig = factory.build(detectorConfigFile);

    long current = System.currentTimeMillis();
    Date endDate = new Date(current - (current % 36_00_000));
    Date startDate = new Date(endDate.getTime() - TimeUnit.HOURS.toMillis(24));

    GenerateAnomalyReport reportGenerator =
        new GenerateAnomalyReport(startDate, endDate, persistenceFile,
            Arrays.asList(config.getDatasets().split(",")), config.getTeBaseUrl(),
            detectorConfig.getSmtpConfiguration(), config.getEmailRecipients());

    reportGenerator.buildReport();
  }

}
