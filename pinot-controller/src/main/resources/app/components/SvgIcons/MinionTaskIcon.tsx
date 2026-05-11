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

export default (props: SvgIconProps) => (
  <SvgIcon
    style={{ width: 24, height: 24, verticalAlign: 'middle' }}
    viewBox="0 0 24 24"
    {...props}
  >
    <path d="M9 2h6l1 3h3a2 2 0 0 1 2 2v3l-3 1v3l3 1v3a2 2 0 0 1-2 2h-3l-1 3H9l-1-3H5a2 2 0 0 1-2-2v-3l3-1v-3L3 10V7a2 2 0 0 1 2-2h3l1-3zm3 6.5A3.5 3.5 0 0 0 8.5 12 3.5 3.5 0 0 0 12 15.5 3.5 3.5 0 0 0 15.5 12 3.5 3.5 0 0 0 12 8.5zm0 2A1.5 1.5 0 0 1 13.5 12 1.5 1.5 0 0 1 12 13.5 1.5 1.5 0 0 1 10.5 12 1.5 1.5 0 0 1 12 10.5z" />
  </SvgIcon>
);
