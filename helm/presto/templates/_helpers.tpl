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
{{- define "presto.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}


{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "presto.fullname" -}}
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
{{- define "presto.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified presto coordinator name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
*/}}
{{- define "presto.coordinator.fullname" -}}
{{ template "presto.fullname" . }}-{{ .Values.coordinator.name }}
{{- end -}}

{{/*
Create a default fully qualified presto worker name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
*/}}
{{- define "presto.worker.fullname" -}}
{{ template "presto.fullname" . }}-{{ .Values.worker.name }}
{{- end -}}

{{/*
The name of the presto coordinator external service.
*/}}
{{- define "presto.coordinator.external" -}}
{{- printf "%s-external" (include "presto.coordinator.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}


{{/*
The name of the presto coordinator headless service.
*/}}
{{- define "presto.coordinator.headless" -}}
{{- printf "%s-headless" (include "presto.coordinator.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
The name of the presto worker headless service.
*/}}
{{- define "presto.worker.headless" -}}
{{- printf "%s-headless" (include "presto.worker.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}