package com.linkedin.thirdeye.datalayer.bao;

import com.linkedin.thirdeye.datalayer.dto.EntityToEntityMappingDTO;
import java.util.List;

public interface EntityToEntityMappingManager extends AbstractManager<EntityToEntityMappingDTO> {
  List<EntityToEntityMappingDTO> findByFromURN(String fromURN);
  List<EntityToEntityMappingDTO> findByToURN(String toURN);
  EntityToEntityMappingDTO findByFromAndToURN(String fromURN, String toURN);
  List<EntityToEntityMappingDTO> findByMappingType(String mappingType);
  List<EntityToEntityMappingDTO> findByFromURNAndMappingType(String fromURN, String mappingType);
  List<EntityToEntityMappingDTO> findByToURNAndMappingType(String toURN, String mappingType);
}
