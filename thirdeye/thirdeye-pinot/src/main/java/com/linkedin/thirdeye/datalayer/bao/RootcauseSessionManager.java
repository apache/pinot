package com.linkedin.thirdeye.datalayer.bao;

import com.linkedin.thirdeye.datalayer.dto.RootcauseSessionDTO;
import java.util.List;
import java.util.Set;


public interface RootcauseSessionManager extends AbstractManager<RootcauseSessionDTO> {
  List<RootcauseSessionDTO> findByName(String name);
  List<RootcauseSessionDTO> findByNameLike(Set<String> nameFragments);
  List<RootcauseSessionDTO> findByOwner(String owner);
  List<RootcauseSessionDTO> findByAnomalyRange(long start, long end);
  List<RootcauseSessionDTO> findByCreatedRange(long start, long end);
  List<RootcauseSessionDTO> findByUpdatedRange(long start, long end);
  List<RootcauseSessionDTO> findByPreviousId(long id);
  List<RootcauseSessionDTO> findByAnomalyId(long id);
}
