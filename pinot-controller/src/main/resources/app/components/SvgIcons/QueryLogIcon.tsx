/**
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

import * as React from 'react';
import SvgIcon, { SvgIconProps } from '@material-ui/core/SvgIcon';

const QueryLogIcon = (props: SvgIconProps) => (
  <SvgIcon style={{ width: 24, height: 24, verticalAlign: 'middle' }} viewBox="0 0 512 512" {...props}>
    <path
      d="M432 64H80C53.5 64 32 85.5 32 112v288c0 26.5 21.5 48 48 48h352c26.5 0 48-21.5 48-48V112c0-26.5-21.5-48-48-48zm16 336c0 8.8-7.2 16-16 16H80c-8.8 0-16-7.2-16-16V112c0-8.8 7.2-16 16-16h352c8.8 0 16 7.2 16 16v288z"
      fill="currentColor"
    />
    <path
      d="M144 160h224v32H144zm0 96h224v32H144zm0 96h160v32H144z"
      fill="currentColor"
    />
  </SvgIcon>
);

export default QueryLogIcon;
