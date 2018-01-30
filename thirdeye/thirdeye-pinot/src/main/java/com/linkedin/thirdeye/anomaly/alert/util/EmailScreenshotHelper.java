package com.linkedin.thirdeye.anomaly.alert.util;

import com.linkedin.thirdeye.alert.content.EmailContentFormatterConfiguration;
import com.linkedin.thirdeye.anomaly.SmtpConfiguration;
import com.linkedin.thirdeye.common.ThirdEyeConfiguration;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmailScreenshotHelper {

  private static final String TEMP_PATH = "/tmp/graph";
  private static final String SCREENSHOT_FILE_SUFFIX = ".png";
  private static final String GRAPH_SCREENSHOT_GENERATOR_SCRIPT = "/getGraphPnj.js";
  private static final Logger LOG = LoggerFactory.getLogger(EmailScreenshotHelper.class);
  private static final ExecutorService executorService = Executors.newCachedThreadPool();

  public static String takeGraphScreenShot(final String anomalyId, final EmailContentFormatterConfiguration configuration) throws JobExecutionException {
    return takeGraphScreenShot(anomalyId, configuration.getDashboardHost(), configuration.getRootDir(),
        configuration.getPhantomJsPath(), configuration.getSmtpConfiguration(), configuration.getFailureFromAddress(),
        configuration.getFailureToAddress());
  }

  public static String takeGraphScreenShot(final String anomalyId, final ThirdEyeConfiguration configuration) throws JobExecutionException {
    return takeGraphScreenShot(anomalyId, configuration.getDashboardHost(), configuration.getRootDir(),
        configuration.getPhantomJsPath(), configuration.getSmtpConfiguration(), configuration.getFailureFromAddress(),
        configuration.getFailureToAddress());
  }

  public static String takeGraphScreenShot(final String anomalyId, final String dashboardHost, final String rootDir,
      final String phantomJsPath, final SmtpConfiguration smtpConfiguration, final String failureFromAddress,
      final String failureToAddress) throws JobExecutionException{
    Callable<String> callable = new Callable<String>() {
      public String call() throws Exception {
        return takeScreenshot(anomalyId, dashboardHost, rootDir, phantomJsPath);
      }
    };
    Future<String> task = executorService.submit(callable);
    String result = null;
    try {
      result = task.get(3, TimeUnit.MINUTES);
      LOG.info("Finished with result: {}", result);
    } catch (Exception e) {
      LOG.error("Exception in fetching screenshot for anomaly id {}", anomalyId, e);
      EmailHelper.sendFailureEmailForScreenshot(anomalyId, e.fillInStackTrace(), smtpConfiguration, failureFromAddress, failureToAddress);
    }
    return result;
  }

  private static String takeScreenshot(String anomalyId, String dashboardHost, String rootDir, String phantomJsPath) throws Exception {

    String imgRoute = dashboardHost + "/app/#/screenshot/" + anomalyId;
    LOG.info("imgRoute {}", imgRoute);
    String phantomScript = rootDir + GRAPH_SCREENSHOT_GENERATOR_SCRIPT;
    LOG.info("Phantom JS script {}", phantomScript);
    String imgPath = TEMP_PATH + anomalyId + SCREENSHOT_FILE_SUFFIX;
    LOG.info("imgPath {}", imgPath);
    Process proc = Runtime.getRuntime().exec(new String[]{phantomJsPath, "phantomjs", "--ssl-protocol=any", "--ignore-ssl-errors=true",
        phantomScript, imgRoute, imgPath});

    StringBuilder sbError = new StringBuilder();
    try (
      InputStream stderr = proc.getErrorStream();
      InputStreamReader isr = new InputStreamReader(stderr);
      BufferedReader br = new BufferedReader(isr);
    ) {
      // exhaust the error stream before waiting for the process to exit
      String line = br.readLine();
      if (line != null) {
        do {
          sbError.append(line);
          sbError.append('\n');

          line = br.readLine();
        } while (line != null);
      }
    }
    int exitVal = proc.waitFor();
    if (exitVal != 0) {
      throw new Exception("PhantomJS process failed with error code " + exitVal + ":\n" + sbError.toString());
    }
    return imgPath;
  }

}
