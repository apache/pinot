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

{{/*
Expand the name of the chart.
*/}}
{{- define "pinot.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "pinot.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}


{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "pinot.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Match Selector labels
*/}}
{{- define "pinot.matchLabels" -}}
app: {{ include "pinot.name" . }}
release: {{ .Release.Name }}
{{- range $key, $value := .Values.additionalMatchLabels }}
{{ $key }}: {{ $value }}
{{- end }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "pinot.labels" -}}
helm.sh/chart: {{ include "pinot.chart" . }}
{{ include "pinot.matchLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
heritage: {{ .Release.Service }}
{{- end }}

{{/*
Broker labels
*/}}
{{- define "pinot.brokerLabels" -}}
{{- include "pinot.labels" . }}
component: {{ .Values.broker.name }}
{{- end }}


{{/*
Controller labels
*/}}
{{- define "pinot.controllerLabels" -}}
{{- include "pinot.labels" . }}
component: {{ .Values.controller.name }}
{{- end }}

{{/*
Minion labels
*/}}
{{- define "pinot.minionLabels" -}}
{{- include "pinot.labels" . }}
component: {{ .Values.minion.name }}
{{- end }}

{{/*
minionStateless labels
*/}}
{{- define "pinot.minionStatelessLabels" -}}
{{- include "pinot.labels" . }}
component: {{ .Values.minionStateless.name }}
{{- end }}

{{/*
Server labels
*/}}
{{- define "pinot.serverLabels" -}}
{{- include "pinot.labels" . }}
component: {{ .Values.server.name }}
{{- end }}



{{/*
Broker Match Selector labels
*/}}
{{- define "pinot.brokerMatchLabels" -}}
{{- include "pinot.matchLabels" . }}
component: {{ .Values.broker.name }}
{{- end }}

{{/*
Controller Match Selector labels
*/}}
{{- define "pinot.controllerMatchLabels" -}}
{{- include "pinot.matchLabels" . }}
component: {{ .Values.controller.name }}
{{- end }}


{{/*
Minion Match Selector labels
*/}}
{{- define "pinot.minionMatchLabels" -}}
{{- include "pinot.matchLabels" . }}
component: {{ .Values.minion.name }}
{{- end }}


{{/*
MinionStateless Match Selector labels
*/}}
{{- define "pinot.minionStatelessMatchLabels" -}}
{{- include "pinot.matchLabels" . }}
component: {{ .Values.minionStateless.name }}
{{- end }}


{{/*
Server Match Selector labels
*/}}
{{- define "pinot.serverMatchLabels" -}}
{{- include "pinot.matchLabels" . }}
component: {{ .Values.server.name }}
{{- end }}


{{/*
Create the name of the service account to use for pinot components
*/}}
{{- define "pinot.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "pinot.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}


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
{{- required "Missing 'zookeeper.urlOverride' entry zookeeper is disabled!"  .Values.zookeeper.urlOverride }}
{{- end -}}
{{- end -}}

{{/*
Zookeeper labels
*/}}
{{- define "pinot.zookeeperLabels" -}}
{{- include "pinot.labels" . }}
component: zookeeper
{{- end }}

{{/*
Zookeeper Match Selector labels
*/}}
{{- define "pinot.zookeeperMatchLabels" -}}
{{- include "pinot.matchLabels" . }}
component: zookeeper
{{- end }}

{{/*
The name of the zookeeper headless service.
*/}}
{{- define "pinot.zookeeper.headless" -}}
{{- printf "%s-headless" (include "pinot.zookeeper.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Generate the ZOO_SERVERS string for the official ZooKeeper Docker image.
Format: server.1=<pod-0>.<headless>.<namespace>:<followerPort>:<electionPort>;<clientPort> server.2=...
*/}}
{{- define "pinot.zookeeper.servers" -}}
{{- $fullname := (include "pinot.zookeeper.fullname" .) -}}
{{- $headless := (include "pinot.zookeeper.headless" .) -}}
{{- $namespace := (include "pinot.namespace" .) -}}
{{- $port := .Values.zookeeper.port | int -}}
{{- $replicas := .Values.zookeeper.replicaCount | int -}}
{{- $servers := list -}}
{{- range $i := until $replicas -}}
{{- $servers = append $servers (printf "server.%d=%s-%d.%s.%s:%d:%d;%d" (add $i 1) $fullname $i $headless $namespace ($.Values.zookeeper.followerPort | int) ($.Values.zookeeper.electionPort | int) $port) -}}
{{- end -}}
{{- join " " $servers -}}
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
Create a default fully qualified pinot minion stateless name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
*/}}
{{- define "pinot.minionStateless.fullname" -}}
{{ template "pinot.fullname" . }}-{{ .Values.minionStateless.name }}
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

{{/*
The name of the pinot minion stateless config.
*/}}
{{- define "pinot.minionStateless.config" -}}
{{- printf "%s-config" (include "pinot.minionStateless.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Return pinot namespace to use
*/}}
{{- define "pinot.namespace" -}}
{{- if .Values.namespaceOverride -}}
    {{- .Values.namespaceOverride -}}
{{- else -}}
    {{- .Release.Namespace -}}
{{- end -}}
{{- end -}}

{{/*
A Kubernetes quantity in bytes. All four JFR size values use this one convention, so `4Gi` means
the same thing everywhere in the chart.

Deliberately rejects the lowercase `m` suffix: in Kubernetes that means *milli*, so `500m` would be
half a byte rather than 500 MB. pinot.jfr.validate rejects it up front with a message that says so.
*/}}
{{- define "pinot.jfr.sizeToBytes" -}}
{{- $value := . | toString -}}
{{- $number := regexFind "^[0-9]+" $value | int64 -}}
{{- $unit := regexFind "[A-Za-z]*$" $value -}}
{{- if eq $unit "Ki" -}}{{ mul $number 1024 }}
{{- else if eq $unit "Mi" -}}{{ mul $number 1048576 }}
{{- else if eq $unit "Gi" -}}{{ mul $number 1073741824 }}
{{- else if eq $unit "Ti" -}}{{ mul $number 1099511627776 }}
{{- else if eq $unit "k" -}}{{ mul $number 1000 }}
{{- else if eq $unit "M" -}}{{ mul $number 1000000 }}
{{- else if eq $unit "G" -}}{{ mul $number 1000000000 }}
{{- else if eq $unit "T" -}}{{ mul $number 1000000000000 }}
{{- else -}}{{ $number }}
{{- end -}}
{{- end -}}

{{/*
A duration in whole minutes, from `<n>m`, `<n>h` or `<n>d`.
*/}}
{{- define "pinot.jfr.durationToMinutes" -}}
{{- $value := . | toString -}}
{{- $number := regexFind "^[0-9]+" $value | int64 -}}
{{- $unit := regexFind "[A-Za-z]*$" $value -}}
{{- if eq $unit "d" -}}{{ mul $number 1440 }}
{{- else if eq $unit "h" -}}{{ mul $number 60 }}
{{- else -}}{{ $number }}
{{- end -}}
{{- end -}}

{{/*
Whether the janitor init container should be rendered for a role.

Call as: include "pinot.jfr.janitorEnabled" (dict "ctx" . "role" .Values.server)

Pass `forceEmptyDir true` for a role that never uses a PersistentVolume (the stateless minion).

More than the two obvious flags. The janitor is only useful on a PersistentVolume: an emptyDir is
created empty with the pod, and volumes are set up before init containers run, so on the emptyDir
path there is provably nothing for it to reclaim. Rendering it there would only add a container to
the serial pod-startup chain.

This lives in a helper rather than inline at each of the five call sites because getting it wrong is
destructive rather than merely wrong: the janitor deletes recordings, so a user who sets
jfr.janitor.enabled=false must actually get no janitor.
*/}}
{{- define "pinot.jfr.janitorEnabled" -}}
{{- if and .role.jfr.enabled .ctx.Values.jfr.janitor.enabled (include "pinot.jfr.usesPersistentVolume" .) -}}
true
{{- end -}}
{{- end -}}

{{/*
Whether a role's recordings actually land on a PersistentVolume.

Not the same as jfr.persistence.enabled: the stateless minion passes `forceEmptyDir true` because a
Deployment cannot give each replica its own volume. Several decisions key off this rather than off
the global flag - whether preserve-repository is worth setting, and whether the janitor has anything
to reclaim.

Call as: include "pinot.jfr.usesPersistentVolume" (dict "ctx" . "role" .Values.server)
*/}}
{{- define "pinot.jfr.usesPersistentVolume" -}}
{{- if and .ctx.Values.jfr.persistence.enabled (not (.forceEmptyDir | default false)) -}}true{{- end -}}
{{- end -}}

{{/*
Validate the shared JFR settings.

JFR options are JVM arguments, so a bad value does not degrade into a warning: the JVM refuses to
start and the pod goes into CrashLoopBackOff with the reason buried in the container log. Catching
it here turns that into a `helm install` error instead.
*/}}
{{- define "pinot.jfr.validate" -}}
{{- $jfr := .Values.jfr -}}
{{- if not $jfr.mountPath -}}
{{- fail "jfr.mountPath must be set when JFR is enabled for any role" -}}
{{- end -}}
{{- if not $jfr.recordingName -}}
{{- fail "jfr.recordingName must be set when JFR is enabled for any role" -}}
{{- end -}}
{{- if not $jfr.configuration -}}
{{- fail "jfr.configuration must be set when JFR is enabled for any role (e.g. 'default' or 'profile')" -}}
{{- end -}}
{{- $sizes := list "maxSize" "maxChunkSize" -}}
{{- range $key := $sizes -}}
{{- $value := index $jfr $key | toString -}}
{{- include "pinot.jfr.validateSize" (dict "key" (printf "jfr.%s" $key) "value" $value) -}}
{{- end -}}
{{- include "pinot.jfr.validateSize" (dict "key" "jfr.persistence.size" "value" ($jfr.persistence.size | toString)) -}}
{{- if $jfr.persistence.emptyDirSizeLimit -}}
{{- include "pinot.jfr.validateSize" (dict "key" "jfr.persistence.emptyDirSizeLimit" "value" ($jfr.persistence.emptyDirSizeLimit | toString)) -}}
{{- end -}}
{{- if $jfr.maxAge -}}
{{- if not (regexMatch "^[0-9]+[smhd]$" ($jfr.maxAge | toString)) -}}
{{- fail (printf "jfr.maxAge must be a duration such as '12h' or '7d', got %q" ($jfr.maxAge | toString)) -}}
{{- end -}}
{{- end -}}
{{- if not (regexMatch "^[0-9]+$" ($jfr.janitor.minIdleMinutes | toString)) -}}
{{- fail (printf "jfr.janitor.minIdleMinutes must be a whole number of minutes, got %q" ($jfr.janitor.minIdleMinutes | toString)) -}}
{{- end -}}
{{- if $jfr.janitor.enabled -}}
{{- if not (or $jfr.janitor.maxAge $jfr.janitor.maxTotalSize) -}}
{{- fail "jfr.janitor is enabled but neither jfr.janitor.maxAge nor jfr.janitor.maxTotalSize is set, so it would never reclaim anything" -}}
{{- end -}}
{{- if $jfr.janitor.maxAge -}}
{{- if not (regexMatch "^[0-9]+[mhd]$" ($jfr.janitor.maxAge | toString)) -}}
{{- fail (printf "jfr.janitor.maxAge must be '<n>m', '<n>h' or '<n>d', got %q" ($jfr.janitor.maxAge | toString)) -}}
{{- end -}}
{{- end -}}
{{- if $jfr.janitor.maxTotalSize -}}
{{- include "pinot.jfr.validateSize" (dict "key" "jfr.janitor.maxTotalSize" "value" ($jfr.janitor.maxTotalSize | toString)) -}}
{{- end -}}
{{- end -}}
{{- include "pinot.jfr.validateBudget" . -}}
{{- end -}}

{{/*
One Kubernetes quantity. Call as: (dict "key" "jfr.maxSize" "value" "2Gi")
*/}}
{{- define "pinot.jfr.validateSize" -}}
{{- if regexMatch "^[0-9]+m$" .value -}}
{{- fail (printf "%s is %q, but a lowercase 'm' means *milli* in Kubernetes units, so that is a fraction of a byte. Use 'M' (10^6) or 'Mi' (2^20)." .key .value) -}}
{{- end -}}
{{- if not (regexMatch "^[0-9]+(k|Ki|M|Mi|G|Gi|T|Ti)?$" .value) -}}
{{- fail (printf "%s must be a Kubernetes quantity such as '512Mi', '2Gi' or '2G', got %q" .key .value) -}}
{{- end -}}
{{- end -}}

{{/*
The JFR volume has to hold what the janitor leaves behind plus the run that starts next. Kubernetes
does not re-run init containers when it restarts a container in place, so each in-place restart
strands another repository; jfr.persistence.restartHeadroom says how many of those to budget for.

Without this check the shipped defaults silently over-commit, and the volume fills during exactly
the incident the recording was enabled to capture. Worse, a full volume stops the JVM from starting
at all, and a container-level restart does not re-run the janitor to recover.
*/}}
{{- define "pinot.jfr.validateBudget" -}}
{{- $jfr := .Values.jfr -}}
{{- if and $jfr.persistence.enabled $jfr.janitor.enabled $jfr.janitor.maxTotalSize -}}
{{- $volume := include "pinot.jfr.sizeToBytes" $jfr.persistence.size | int64 -}}
{{- $budget := include "pinot.jfr.sizeToBytes" $jfr.janitor.maxTotalSize | int64 -}}
{{- $run := include "pinot.jfr.sizeToBytes" $jfr.maxSize | int64 -}}
{{- $chunk := include "pinot.jfr.sizeToBytes" $jfr.maxChunkSize | int64 -}}
{{- $runs := add1 ($jfr.persistence.restartHeadroom | int64) -}}
{{- $needed := add $budget (mul $runs (add $run (mul $chunk 2))) -}}
{{- if gt $needed $volume -}}
{{- fail (printf "JFR volume is over-committed. The janitor trims down to jfr.janitor.maxTotalSize (%s), then each JVM run writes up to jfr.maxSize (%s) plus two chunks of jfr.maxChunkSize (%s). Budgeting %d run(s) (1 + jfr.persistence.restartHeadroom) needs %d bytes, but jfr.persistence.size (%s) provides only %d. Raise jfr.persistence.size, or lower jfr.janitor.maxTotalSize or jfr.persistence.restartHeadroom." ($jfr.janitor.maxTotalSize | toString) ($jfr.maxSize | toString) ($jfr.maxChunkSize | toString) $runs $needed ($jfr.persistence.size | toString) $volume) -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
JVM arguments that start the continuous recording. Appended to JAVA_OPTS.

Sizes are rendered as byte counts, which JFR accepts, so that the chart can expose Kubernetes
quantities everywhere and never make the user think about JFR's own unit table.

Call as: include "pinot.jfr.javaOpts" (dict "ctx" . "role" .Values.server)

`preserve-repository` tracks whether this role's recordings land on a PersistentVolume. There it is
the whole point:
without it JFR deletes the repository on a clean shutdown, and a clean shutdown is what Kubernetes
asks for first. On an emptyDir it would be actively harmful — the volume outlives an in-place
container restart but the janitor does not run then, so preserved repositories would pile up with
nothing able to reclaim them.

`dumponexit=false` is deliberate. The repository is already the durable copy, and a dump on exit
only produces a second one; after a SIGKILL there is no exit hook to run anyway. Use
`jfr assemble <repository-dir> out.jfr` to turn a leftover repository into a single recording,
including the chunk that was open when the JVM died.
*/}}
{{- define "pinot.jfr.javaOpts" -}}
{{- include "pinot.jfr.validate" .ctx -}}
{{- $jfr := .ctx.Values.jfr -}}
{{- $preserve := include "pinot.jfr.usesPersistentVolume" . -}}
{{- $recording := list
      (printf "name=%s" $jfr.recordingName)
      (printf "settings=%s" $jfr.configuration)
      "disk=true"
      (printf "maxsize=%s" (include "pinot.jfr.sizeToBytes" $jfr.maxSize))
      "dumponexit=false" -}}
{{- if $jfr.maxAge -}}
{{- $recording = append $recording (printf "maxage=%s" ($jfr.maxAge | toString)) -}}
{{- end -}}
{{- $options := list
      (printf "repository=%s" $jfr.mountPath)
      (printf "preserve-repository=%t" (eq $preserve "true"))
      (printf "maxchunksize=%s" (include "pinot.jfr.sizeToBytes" $jfr.maxChunkSize)) -}}
{{- printf "-XX:FlightRecorderOptions=%s -XX:StartFlightRecording=%s" (join "," $options) (join "," $recording) -}}
{{- end -}}

{{/*
Mount for the JFR repository. A volume of its own, never a subdirectory of the role's data volume,
so a recording can never compete with segments for space.
*/}}
{{- define "pinot.jfr.volumeMount" -}}
- name: jfr
  mountPath: {{ .Values.jfr.mountPath | quote }}
{{- end -}}

{{/*
Pod-level JFR volume, used when the recording is not kept on a PersistentVolume. With persistence
enabled the StatefulSet roles get theirs from volumeClaimTemplates instead.

The emptyDir is always given a sizeLimit so that a runaway recording is the kubelet's problem rather
than the node's: without one it draws on shared node ephemeral storage and can trigger DiskPressure
evictions of unrelated pods.
*/}}
{{- define "pinot.jfr.emptyDirVolume" -}}
{{- $jfr := .Values.jfr -}}
{{- /* takes the root context: the sizeLimit does not depend on the role */ -}}
{{- $limit := $jfr.persistence.emptyDirSizeLimit -}}
{{- if not $limit -}}
{{- $run := include "pinot.jfr.sizeToBytes" $jfr.maxSize | int64 -}}
{{- $chunk := include "pinot.jfr.sizeToBytes" $jfr.maxChunkSize | int64 -}}
{{- $limit = printf "%d" (add $run (mul $chunk 2)) -}}
{{- end -}}
- name: jfr
  emptyDir:
    sizeLimit: {{ $limit | quote }}
{{- end -}}

{{/*
volumeClaimTemplates entry for the JFR repository.
*/}}
{{- define "pinot.jfr.volumeClaimTemplate" -}}
- metadata:
    name: jfr
  spec:
    accessModes:
      - {{ .Values.jfr.persistence.accessMode | quote }}
    {{- if .Values.jfr.persistence.storageClass }}
    {{- if (eq "-" .Values.jfr.persistence.storageClass) }}
    storageClassName: ""
    {{- else }}
    storageClassName: {{ .Values.jfr.persistence.storageClass }}
    {{- end }}
    {{- end }}
    resources:
      requests:
        storage: {{ .Values.jfr.persistence.size }}
{{- end -}}

{{/*
The role's initContainers list: the JFR janitor (when it applies) followed by whatever the user
configured in <role>.initContainers.

Call as: include "pinot.jfr.initContainers" (dict "ctx" . "role" .Values.server)

Renders nothing at all when there is neither, so that roles without init containers keep producing
exactly the manifest they did before - including no stray blank line, which is why the pod-spec
indentation is baked in here rather than applied with `nindent` at the call site.
*/}}
{{- define "pinot.jfr.initContainers" -}}
{{- $extra := .role.initContainers | default list -}}
{{- $janitor := include "pinot.jfr.janitorEnabled" . -}}
{{- if or $janitor $extra }}
      initContainers:
        {{- if $janitor }}
        {{- include "pinot.jfr.janitorInitContainer" .ctx | nindent 8 }}
        {{- end }}
        {{- if $extra }}
        {{- toYaml $extra | nindent 8 }}
        {{- end }}
{{- end }}
{{- end -}}

{{/*
Init container that reclaims JFR repositories left behind by previous JVM runs.

JFR's own `maxsize` bounds the repository of the JVM that is running; nothing in the JVM ever
reclaims the repository of a JVM that has already exited. On a PersistentVolume those directories
survive every restart and accumulate without limit, so the reclaiming has to happen outside the JVM.

Running it as an init container is what makes it safe: init containers finish before the Pinot
container starts, so on a volume owned by a single pod every directory belongs to a run that is
already over. The script does not rely on that alone — it also refuses to delete a repository that
was written to within jfr.janitor.minIdleMinutes.

Sizes and durations are converted here and passed as plain integers, so the script parses no units
and there is one source of truth for the unit table.
*/}}
{{- define "pinot.jfr.janitorInitContainer" -}}
{{- $jfr := .Values.jfr -}}
- name: jfr-janitor
  image: "{{ $jfr.janitor.image.repository | default .Values.image.repository }}:{{ $jfr.janitor.image.tag | default .Values.image.tag }}"
  imagePullPolicy: {{ $jfr.janitor.image.pullPolicy | default .Values.image.pullPolicy }}
  {{- with $jfr.janitor.securityContext }}
  securityContext:
    {{- toYaml . | nindent 4 }}
  {{- end }}
  env:
    - name: PINOT_JFR_REPOSITORY
      value: {{ $jfr.mountPath | quote }}
    - name: PINOT_JFR_JANITOR_MAX_AGE_MINUTES
      value: {{ if $jfr.janitor.maxAge }}{{ include "pinot.jfr.durationToMinutes" $jfr.janitor.maxAge | quote }}{{ else }}""{{ end }}
    - name: PINOT_JFR_JANITOR_MAX_TOTAL_KIB
      value: {{ if $jfr.janitor.maxTotalSize }}{{ div (include "pinot.jfr.sizeToBytes" $jfr.janitor.maxTotalSize | int64) 1024 | quote }}{{ else }}""{{ end }}
    - name: PINOT_JFR_JANITOR_MIN_IDLE_MINUTES
      value: {{ $jfr.janitor.minIdleMinutes | toString | quote }}
  command:
    - /bin/sh
    - -c
    - |
      {{- .Files.Get "scripts/jfr-janitor.sh" | nindent 6 }}
  volumeMounts:
    {{- include "pinot.jfr.volumeMount" . | nindent 4 }}
  {{- with $jfr.janitor.resources }}
  resources:
    {{- toYaml . | nindent 4 }}
  {{- end }}
{{- end -}}
