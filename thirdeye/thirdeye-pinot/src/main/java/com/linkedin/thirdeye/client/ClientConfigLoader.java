package com.linkedin.thirdeye.client;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

public class ClientConfigLoader {
 private static final Logger LOG = LoggerFactory.getLogger(ClientConfigLoader.class);
 private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());

 public static ClientConfig fromClientConfigPath(String clientConfigPath) {
   ClientConfig clientConfig = null;
   try {
     clientConfig = OBJECT_MAPPER.readValue(new File(clientConfigPath), ClientConfig.class);
   } catch (IOException e) {
     LOG.error("Exception in reading client config file {}", clientConfigPath, e);
   }
   return clientConfig;
 }

 public static Map<String, ThirdEyeClient> getClientMap(ClientConfig clientConfig) {
   Map<String, ThirdEyeClient> clientMap = new HashMap<>();
   for (Client client : clientConfig.getClients()) {
     String className = client.getClassName();
     Map<String, String> properties = client.getProperties();
     try {
       LOG.info("Creating thirdeye client {} with properties '{}'", className, properties);
       Constructor<?> constructor = Class.forName(className).getConstructor(Map.class);
       ThirdEyeClient thirdeyeClient = (ThirdEyeClient) constructor.newInstance(properties);
       String name = thirdeyeClient.getName();
       clientMap.put(name, thirdeyeClient);
     } catch (Exception e) {
       LOG.error("Exception in creating thirdeye client {}", className, e);
     }
   }
   return clientMap;
 }

}
