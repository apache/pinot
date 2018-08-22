package com.linkedin.thirdeye.detection.yaml;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.wordnik.swagger.annotations.ApiParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;


@Path("/yaml")
public class YamlTranslationResource {

  @POST
  public Response translateYamlToDetectionConfig(@ApiParam("payload") String payload) throws Exception {
      if (payload == null) {
      throw new IllegalArgumentException("Empty Payload");
    }
    YamlDetectionConfigTranslator translator = new YamlDetectionConfigTranslator();
    DetectionConfigDTO config = translator.convertYamlToDetectionConfig(payload);
    ObjectMapper objectMapper = new ObjectMapper();
    return Response.ok(objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(config)).build();
  }

}
