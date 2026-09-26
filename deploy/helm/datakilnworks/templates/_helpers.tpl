{{- define "dkw.name" -}}{{- default .Chart.Name .Values.nameOverride | trunc 40 | trimSuffix "-" -}}{{- end -}}
{{- define "dkw.fullname" -}}
{{- if .Values.fullnameOverride -}}{{- .Values.fullnameOverride | trunc 50 | trimSuffix "-" -}}
{{- else -}}{{- $n := default .Chart.Name .Values.nameOverride -}}{{- if contains $n .Release.Name -}}{{- .Release.Name | trunc 50 | trimSuffix "-" -}}{{- else -}}{{- printf "%s-%s" .Release.Name $n | trunc 50 | trimSuffix "-" -}}{{- end -}}{{- end -}}
{{- end -}}
{{- define "dkw.labels" -}}
app.kubernetes.io/name: {{ include "dkw.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}
{{- define "dkw.studioSelector" -}}
app.kubernetes.io/name: datakilnworks-studio
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
{{- define "dkw.secretName" -}}{{- default (printf "%s-secrets" (include "dkw.fullname" .)) .Values.secrets.existingSecret -}}{{- end -}}
{{- define "dkw.image" -}}{{ .Values.image.repository }}:{{ .Values.image.tag }}{{- end -}}
{{- define "dkw.claim" -}}{{- $c := index .root.Values.persistence .key -}}{{- default (printf "%s-%s" (include "dkw.fullname" .root) (lower .key | replace "_" "-")) $c.existingClaim -}}{{- end -}}
