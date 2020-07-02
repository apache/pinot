/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import React, {useState} from 'react';

import CheckboxList from '@site/src/components/CheckboxList';

import _ from 'lodash';

import './styles.css';

function Fields({children, filters}) {
  const [onlyCommon, setOnlyCommon] = useState(false);
  const [onlyGroups, setOnlyGroups] = useState(new Set());
  const [onlyRequired, setOnlyRequired] = useState(false);
  const [searchTerm, setSearchTerm] = useState(null);

  let childrenArray = [];

  if (children) {
    childrenArray = Array.isArray(children) ? children : [children];
  }

  let commonRelevant = childrenArray.some(child => child.props.common);
  let groups = _(childrenArray).flatMap(child => child.props.groups).uniq().value();
  let requiredRelevant = childrenArray.some(child => child.props.required);
  let filteredChildren = childrenArray;

  if (onlyCommon) {
    filteredChildren = filteredChildren.filter(child => child.props.common);
  }

  if (onlyGroups.size > 0) {
    filteredChildren = filteredChildren.filter(child => Array.from(onlyGroups).every(group => child.props.groups.includes(group)));
  }

  if (onlyRequired) {
    filteredChildren = filteredChildren.filter(child => child.props.required);
  }

  if (searchTerm) {
    filteredChildren = filteredChildren.filter(child =>
      child.props.name.toLowerCase().includes(searchTerm.toLowerCase())
    );
  }

  return (
    <div className="fields">
      {childrenArray.length > 1 && filters !== false ?
        (<div className="filters">
          <span className="result-count">{filteredChildren.length} items</span>
          <div className=" search">
            <input
              type="text"
              onChange={(event) => setSearchTerm(event.currentTarget.value)}
              placeholder="🔍 Search..." />
          </div>
          <div className="checkboxes">
            <CheckboxList
              values={groups}
              currentState={onlyGroups}
              setState={setOnlyGroups} />
            {commonRelevant && (
              <label title="Only show popular/common results">
              <input
                type="checkbox"
                onChange={(event) => setOnlyCommon(event.currentTarget.checked)}
                checked={onlyCommon} />
              common only
            </label>)}
            {requiredRelevant && (
              <label title="Only show required results">
              <input
                type="checkbox"
                onChange={(event) => setOnlyRequired(event.currentTarget.checked)}
                checked={onlyRequired} />
              required only
            </label>)}
          </div>
        </div>) :
        null}
      <div className="section-list">
        {!Array.isArray(filteredChildren) || filteredChildren.length > 0 ?
          filteredChildren :
          <div className="empty">
            <div className="icon">☹</div>
            <div>No fields found</div>
          </div>}
      </div>
    </div>
  );
}

export default Fields;
