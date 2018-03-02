package com.linkedin.thirdeye.dashboard.resources;

import com.linkedin.thirdeye.anomaly.detection.AnomalyDetectionInputContext;
import com.linkedin.thirdeye.anomaly.detection.AnomalyDetectionInputContextBuilder;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.constant.MetricAggFunction;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import com.linkedin.thirdeye.detector.function.AnomalyFunction;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;
import com.linkedin.thirdeye.detector.function.BaseAnomalyFunction;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.io.IOUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Produces(MediaType.APPLICATION_JSON)
public class AnomalyFunctionResource {

  private static final Logger LOG = LoggerFactory.getLogger(AnomalyFunctionResource.class);

  private final Map<String, Object> anomalyFunctionMetadata = new HashMap<>();
  private final AnomalyFunctionFactory anomalyFunctionFactory;

  public AnomalyFunctionResource(String functionConfigPath) {
    buildFunctionMetadata(functionConfigPath);
    this.anomalyFunctionFactory = new AnomalyFunctionFactory(functionConfigPath);
  }

  private void buildFunctionMetadata(String functionConfigPath) {
    Properties props = new Properties();
    InputStream input = null;
    try {
      input = new FileInputStream(functionConfigPath);
      props.load(input);
    } catch (IOException e) {
      LOG.error("Function config not found at {}", functionConfigPath);
    } finally {
      IOUtils.closeQuietly(input);
    }
    LOG.info("Loaded functions : " + props.keySet() + " from path : " + functionConfigPath);
    for (Object key : props.keySet()) {
      String functionName = key.toString();
      try {
        Class<AnomalyFunction> clz = (Class<AnomalyFunction>) Class.forName(props.get(functionName).toString());
        Method getFunctionProps = clz.getMethod("getPropertyKeys");
        AnomalyFunction anomalyFunction = clz.newInstance();
        anomalyFunctionMetadata.put(functionName, getFunctionProps.invoke(anomalyFunction));
      } catch (ClassNotFoundException e) {
        LOG.warn("Unknown class for function : " + functionName);
      } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
        LOG.error("Unknown method", e);
      } catch (InstantiationException e) {
        LOG.error("Unsupported anomaly function", e);
      }
    }
  }
}
