/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
