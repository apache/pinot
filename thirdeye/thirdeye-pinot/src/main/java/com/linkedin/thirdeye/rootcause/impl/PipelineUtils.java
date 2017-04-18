package com.linkedin.thirdeye.rootcause.impl;

import java.io.FileInputStream;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.common.ThirdEyeConfiguration;
import com.linkedin.thirdeye.rootcause.Pipeline;

public class PipelineUtils {

  private static final Logger LOG = LoggerFactory.getLogger(PipelineUtils.class);

  public static List<Pipeline> getPipelinesFromConfig(ThirdEyeConfiguration thirdeyeConfig) {
    List<Pipeline> pipelines = new ArrayList<>();
    Properties props = new Properties();
    try (InputStream input = new FileInputStream(thirdeyeConfig.getRCAPipelinesConfigPath());) {
      props.load(input);
    } catch (Exception e) {
      LOG.error("Exception in loading pipelines file", e);
    }

    for (Entry<Object, Object> entry : props.entrySet()) {
      try {
        String className = (String) entry.getValue();
        Constructor<?> constructor = Class.forName(className).getConstructor();
        Pipeline pipeline = (Pipeline) constructor.newInstance();
        pipelines.add(pipeline);
      } catch (Exception e) {
        LOG.error("Exception in instantiating pipeline for {}:{}", entry.getKey(), entry.getValue(), e);
      }
    }
    return pipelines;
  }
}
