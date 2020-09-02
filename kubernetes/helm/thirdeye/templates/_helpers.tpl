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
{{- define "thirdeye.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "thirdeye.fullname" -}}
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
{{- define "thirdeye.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified thirdeye frontend name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
*/}}
{{- define "thirdeye.frontend.fullname" -}}
{{ template "thirdeye.fullname" . }}-{{ .Values.frontend.name }}
{{- end -}}

{{/*
Create a default fully qualified thirdeye backend name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
*/}}
{{- define "thirdeye.backend.fullname" -}}
{{ template "thirdeye.fullname" . }}-{{ .Values.backend.name }}
{{- end -}}

{{/*
Create a default fully qualified thirdeye scheduler (backend with special detector.yml) name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
*/}}
{{- define "thirdeye.scheduler.fullname" -}}
{{ template "thirdeye.fullname" . }}-{{ .Values.scheduler.name }}
{{- end -}}

{{/*
The name of the thirdeye config.
*/}}
{{- define "thirdeye.config" -}}
{{- printf "%s-config" (include "thirdeye.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
The name of the thirdeye scheduler (backend with special detector.yml) config.
*/}}
{{- define "thirdeye.scheduler.config" -}}
{{- printf "%s-scheduler-config" (include "thirdeye.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
The name of the thirdeye frontend external service.
*/}}
{{- define "thirdeye.frontend.external" -}}
{{- printf "%s-external" (include "thirdeye.frontend.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
The name of the thirdeye frontend headless service.
*/}}
{{- define "thirdeye.frontend.headless" -}}
{{- printf "%s-headless" (include "thirdeye.frontend.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
The name of the thirdeye backend headless service.
*/}}
{{- define "thirdeye.backend.headless" -}}
{{- printf "%s-headless" (include "thirdeye.backend.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
The name of the thirdeye scheduler (backend with special detector.yml) headless service.
*/}}
{{- define "thirdeye.scheduler.headless" -}}
{{- printf "%s-headless" (include "thirdeye.scheduler.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
  Create a default fully qualified traefik name.
  We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
*/}}
{{- define "thirdeye.traefik.fullname" -}}
{{-   if .Values.traefik.fullnameOverride -}}
{{-     .Values.traefik.fullnameOverride | trunc -63 | trimSuffix "-" -}}
{{-   else -}}
{{-     $name := default "traefik" .Values.traefik.nameOverride -}}
{{-     printf "%s-%s" .Release.Name $name | trunc -63 | trimSuffix "-" -}}
{{-    end -}}
{{- end -}}

