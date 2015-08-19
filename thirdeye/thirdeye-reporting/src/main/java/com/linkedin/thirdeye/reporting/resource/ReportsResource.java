package com.linkedin.thirdeye.reporting.resource;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import io.dropwizard.lifecycle.Managed;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import com.sun.jersey.api.ConflictException;
import com.sun.jersey.api.NotFoundException;
import com.codahale.metrics.MetricRegistry;
import com.linkedin.thirdeye.reporting.api.ReportConfig;
import com.linkedin.thirdeye.reporting.api.ReportConstants;


@Path("/reports")
@Produces(MediaType.APPLICATION_JSON)
public class ReportsResource implements Managed {

  private MetricRegistry metricsRegistry;
  private String reportConfigPath;

  public ReportsResource(MetricRegistry metrics, String reportConfigPath) {
    this.metricsRegistry = metrics;
    this.reportConfigPath = reportConfigPath;
  }

  @Override
  public void start() throws Exception {
    // TODO Auto-generated method stub

  }

  @Override
  public void stop() throws Exception {
    // TODO Auto-generated method stub

  }

  @GET
  public List<String> getReports() {
    File reportConfigFolder = new File(reportConfigPath);
    String[] reportConfigFiles = reportConfigFolder.list(new FilenameFilter() {

      @Override
      public boolean accept(File dir, String name) {
        return name.endsWith(ReportConstants.YAML_FILE_SUFFIX);
      }
    });
    if (reportConfigFiles == null || reportConfigFiles.length == 0) {
      throw new NotFoundException("No report config files at rootDir " + reportConfigPath);
    }
    return Arrays.asList(reportConfigFiles);
  }

  @GET
  @Path("/{report}")
  public ReportConfig getReportConfig(@PathParam("report") String reportFileName) throws FileNotFoundException, IOException {
    File reportConfigFile = new File(reportConfigPath, reportFileName);
    if (!reportConfigFile.exists()) {
      throw new NotFoundException("Report config " + reportFileName + " does not exist");
    }
    return ReportConfig.decode(new FileInputStream(reportConfigFile));
  }

  @DELETE
  @Path("/{report}")
  public Response deleteReportConfig(@PathParam("report") String reportFileName) {
    File reportConfigFile = new File(reportConfigPath, reportFileName);
    if (!reportConfigFile.exists()) {
      throw new NotFoundException("Report config " + reportFileName + "does not exist");
    }
    reportConfigFile.delete();
    return Response.noContent().build();
  }

  @POST
  @Path("/{report}")
  @Consumes(MediaType.APPLICATION_OCTET_STREAM)
  public Response postConfig(@PathParam("report") String reportFileName, byte[] configBytes) throws IOException
  {
    File reportConfigFolder = new File(reportConfigPath);
    if (!reportConfigFolder.exists())
    {
      FileUtils.forceMkdir(reportConfigFolder);
    }

    File reportConfigFile = new File(reportConfigFolder, reportFileName);

    if (!reportConfigFile.exists())
    {
      IOUtils.copy(new ByteArrayInputStream(configBytes), new FileOutputStream(reportConfigFile));
    }
    else
    {
      throw new ConflictException(reportConfigFile.getPath()+" already exists. A DELETE of /reports/{report} is required first");
    }
    return Response.ok().build();
  }

}
