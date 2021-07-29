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

/* eslint-disable no-console */

import axios from 'axios';
import { AuthWorkflow } from 'Models';
import app_state from '../app_state';

const isDev = process.env.NODE_ENV !== 'production';

const handleError = (error: any) => {
  if (isDev) {
    console.log(error);
  }
  return error.response || error;
};

const handleResponse = (response: any) => {
  if (isDev) {
    console.log(response);
  }
  return response;
};

const handleConfig = (config: any) => {
  // Attach auth token for basic auth
  if (app_state.authWorkflow === AuthWorkflow.BASIC && app_state.authToken) {
    Object.assign(config.headers, { Authorization: app_state.authToken });
  }

  // Attach auth token for OIDC auth
  if (app_state.authWorkflow === AuthWorkflow.OIDC && app_state.authToken) {
    Object.assign(config.headers, {
      Authorization: `Bearer ${app_state.authToken}`,
    });
  }

  if (isDev) {
    console.log(config);
  }

  return config;
};

export const baseApi = axios.create({ baseURL: '/' });
baseApi.interceptors.request.use(handleConfig, handleError);
baseApi.interceptors.response.use(handleResponse, handleError);

export const transformApi = axios.create({baseURL: '/', transformResponse: [data => data]});