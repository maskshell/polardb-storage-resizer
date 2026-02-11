{{/*
Expand the name of the chart.
*/}}
{{- define "polardb-resizer.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "polardb-resizer.fullname" -}}
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
{{- define "polardb-resizer.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "polardb-resizer.labels" -}}
helm.sh/chart: {{ include "polardb-resizer.chart" . }}
{{ include "polardb-resizer.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "polardb-resizer.selectorLabels" -}}
app.kubernetes.io/name: {{ include "polardb-resizer.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "polardb-resizer.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "polardb-resizer.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Create the namespace
*/}}
{{- define "polardb-resizer.namespace" -}}
{{- default .Release.Namespace .Values.global.namespaceOverride }}
{{- end }}

{{/*
Create image tag
*/}}
{{- define "polardb-resizer.imageTag" -}}
{{- default .Chart.AppVersion .Values.image.tag }}
{{- end }}

{{/*
Create full image name
*/}}
{{- define "polardb-resizer.image" -}}
{{- $registryName := required "image.repository is required — set it to your container registry path" .Values.image.repository -}}
{{- $tag := include "polardb-resizer.imageTag" . -}}
{{- printf "%s:%s" $registryName $tag -}}
{{- end }}

{{/*
RRSA annotations for ServiceAccount
Reference: https://help.aliyun.com/zh/ack/serverless-kubernetes/user-guide/use-rrsa-to-authorize-pods-to-access-different-cloud-services
*/}}
{{- define "polardb-resizer.rrsaAnnotations" -}}
{{- if .Values.rrsa.enabled }}
{{- if .Values.rrsa.roleName }}
pod-identity.alibabacloud.com/role-name: {{ .Values.rrsa.roleName | quote }}
{{- end }}
{{- end }}
{{- end }}
