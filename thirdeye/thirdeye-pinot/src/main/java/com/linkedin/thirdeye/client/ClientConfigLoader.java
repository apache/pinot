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

/**
 * This class contains helper classes to load/transform clients from file
 */
public class ClientConfigLoader {
 private static final Logger LOG = LoggerFactory.getLogger(ClientConfigLoader.class);
 private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());

 /**
  * Returns client config from yml file
  * @param clientConfigPath
  * @return ClientConfig
  */
 public static ClientConfig fromClientConfigPath(String clientConfigPath) {
   ClientConfig clientConfig = null;
   try {
     clientConfig = OBJECT_MAPPER.readValue(new File(clientConfigPath), ClientConfig.class);
   } catch (IOException e) {
     LOG.error("Exception in reading client config file {}", clientConfigPath, e);
   }
   return clientConfig;
 }

 /**
  * Returns client name to client map
  * @param clientConfig
  * @return
  */
 public static Map<String, ThirdEyeClient> getClientMap(ClientConfig clientConfig) {
   Map<String, ThirdEyeClient> clientMap = new HashMap<>();
   for (Client client : clientConfig.getClients()) {
     String className = client.getClassName();
     Map<String, String> properties = client.getProperties();
     try {
       LOG.info("Creating thirdeye client {} with properties '{}'", className, properties);
       Constructor<?> constructor = Class.forName(className).getConstructor(Map.class);
       ThirdEyeClient thirdeyeClient = (ThirdEyeClient) constructor.newInstance(properties);
       // use class simple name as key, this enforces that there cannot be more than one client of the same type
       String name = thirdeyeClient.getName();
       if (clientMap.containsKey(name)) {
         throw new IllegalStateException("Client " + name + " already exists. "
             + "There can be only ONE client of each type");
       }
       clientMap.put(name, thirdeyeClient);
     } catch (Exception e) {
       LOG.error("Exception in creating thirdeye client {}", className, e);
     }
   }
   return clientMap;
 }

}
