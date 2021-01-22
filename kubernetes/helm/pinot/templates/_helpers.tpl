#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

{{/* vim: set filetype=mustache: */}}
{{/*
Expand the name of the chart.
*/}}
{{- define "pinot.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}


{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "pinot.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}


{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "pinot.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}


{{/*
Create a default fully qualified zookeeper name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
*/}}
{{- define "pinot.zookeeper.fullname" -}}
{{- if .Values.zookeeper.fullnameOverride -}}
{{- .Values.zookeeper.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default "zookeeper" .Values.zookeeper.nameOverride -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{/*
Form the Zookeeper URL. If zookeeper is installed as part of this chart, use k8s service discovery,
else use user-provided URL
*/}}
{{- define "zookeeper.url" }}
{{- $port := .Values.zookeeper.port | toString }}
{{- if .Values.zookeeper.enabled -}}
{{- printf "%s:%s" (include "pinot.zookeeper.fullname" .) $port }}
{{- else -}}
{{- $zookeeperConnect := printf "%s:%s" .Values.zookeeper.url $port }}
{{- $zookeeperConnectOverride := index .Values "configurationOverrides" "zookeeper.connect" }}
{{- default $zookeeperConnect $zookeeperConnectOverride }}
{{- end -}}
{{- end -}}

{{/*
Create a default fully qualified pinot controller name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
*/}}
{{- define "pinot.controller.fullname" -}}
{{ template "pinot.fullname" . }}-{{ .Values.controller.name }}
{{- end -}}


{{/*
Create a default fully qualified pinot broker name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
*/}}
{{- define "pinot.broker.fullname" -}}
{{ template "pinot.fullname" . }}-{{ .Values.broker.name }}
{{- end -}}


{{/*
Create a default fully qualified pinot server name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
*/}}
{{- define "pinot.server.fullname" -}}
{{ template "pinot.fullname" . }}-{{ .Values.server.name }}
{{- end -}}


{{/*
Create a default fully qualified pinot minion name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
*/}}
{{- define "pinot.minion.fullname" -}}
{{ template "pinot.fullname" . }}-{{ .Values.minion.name }}
{{- end -}}

{{/*
The name of the pinot controller headless service.
*/}}
{{- define "pinot.controller.headless" -}}
{{- printf "%s-headless" (include "pinot.controller.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
The name of the pinot broker headless service.
*/}}
{{- define "pinot.broker.headless" -}}
{{- printf "%s-headless" (include "pinot.broker.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
The name of the pinot server headless service.
*/}}
{{- define "pinot.server.headless" -}}
{{- printf "%s-headless" (include "pinot.server.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
The name of the pinot minion headless service.
*/}}
{{- define "pinot.minion.headless" -}}
{{- printf "%s-headless" (include "pinot.minion.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
The name of the pinot controller external service.
*/}}
{{- define "pinot.controller.external" -}}
{{- printf "%s-external" (include "pinot.controller.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
The name of the pinot broker external service.
*/}}
{{- define "pinot.broker.external" -}}
{{- printf "%s-external" (include "pinot.broker.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
The name of the pinot controller config.
*/}}
{{- define "pinot.controller.config" -}}
{{- printf "%s-config" (include "pinot.controller.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
The name of the pinot broker config.
*/}}
{{- define "pinot.broker.config" -}}
{{- printf "%s-config" (include "pinot.broker.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
The name of the pinot server config.
*/}}
{{- define "pinot.server.config" -}}
{{- printf "%s-config" (include "pinot.server.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
The name of the pinot minion config.
*/}}
{{- define "pinot.minion.config" -}}
{{- printf "%s-config" (include "pinot.minion.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
