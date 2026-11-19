#!/bin/bash
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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -euo pipefail

image=apachepinot/pinot
digest_pattern='^sha256:[0-9a-f]{64}$'

die() {
  printf '%s\n' "$*" >&2
  exit 1
}

image_config() {
  jq -Sc '{User: (.User // ""), Entrypoint: (.Entrypoint // []), Cmd: (.Cmd // []),
    WorkingDir: (.WorkingDir // ""), Env: (.Env // []), Labels: (.Labels // {}),
    ExposedPorts: (.ExposedPorts // {}), Volumes: (.Volumes // {}), Healthcheck,
    StopSignal: (.StopSignal // ""), Shell: (.Shell // [])}'
}

manifest() {
  docker buildx imagetools inspect "$image:$1" --format '{{json .Manifest}}'
}

digest_of() {
  manifest "$1" | jq -er '.digest'
}

config_of() {
  docker buildx imagetools inspect "$image@$1" --format '{{json .Image}}'
}

build_patch() (
  context=$(mktemp -d)
  trap 'rm -rf -- "$context"' EXIT
  local -a args=(--build-arg "BASE_IMAGE=$image@$OLD_DIGEST" --build-arg "JDK_MAJOR=$MAJOR")
  case "$VENDOR" in
    ms-openjdk)
      [[ "$MS_DONOR" =~ ^mcr\.microsoft\.com/openjdk/jdk@sha256:[0-9a-f]{64}$ ]] || die 'Invalid Microsoft JDK donor'
      args+=(--build-arg "JDK_DONOR=$MS_DONOR")
      cat > "$context/Dockerfile" <<'EOF'
ARG BASE_IMAGE
ARG JDK_DONOR
FROM ${JDK_DONOR} AS jdk
FROM ${BASE_IMAGE}
ARG JDK_MAJOR
RUN test "$JAVA_HOME" = "/usr/lib/jvm/msopenjdk-$JDK_MAJOR" && rm -rf -- "$JAVA_HOME"
COPY --from=jdk /usr/lib/jvm/msopenjdk-${JDK_MAJOR} /usr/lib/jvm/msopenjdk-${JDK_MAJOR}
EOF
      ;;
    amazoncorretto)
      [[ -n "$CORRETTO_PACKAGE_VERSION" ]] || die 'Missing Corretto package version'
      args+=(--build-arg "JDK_PACKAGE_VERSION=$CORRETTO_PACKAGE_VERSION")
      cat > "$context/Dockerfile" <<'EOF'
ARG BASE_IMAGE
FROM ${BASE_IMAGE}
ARG JDK_MAJOR
ARG JDK_PACKAGE_VERSION
RUN set -eux; \
    test "$JAVA_HOME" = "/usr/lib/jvm/java-${JDK_MAJOR}-amazon-corretto"; \
    package="java-${JDK_MAJOR}-amazon-corretto"; \
    . /etc/os-release; \
    case "$ID:$VERSION_ID" in \
      debian:*) \
        apt-get update; \
        apt-get install -y --no-install-recommends --only-upgrade "$package-jdk=$JDK_PACKAGE_VERSION"; \
        rm -rf /var/lib/apt/lists/*; \
        ;; \
      amzn:2023) \
        rpm_nvr="${JDK_PACKAGE_VERSION#*:}"; \
        rpm_version="${rpm_nvr%-*}"; \
        rpm_release=amzn2023; \
        if [ "$JDK_MAJOR" != 11 ]; then rpm_release=amzn2023.1; fi; \
        rpm_arch=$(rpm -q --qf '%{ARCH}' rpm); \
        rpm_dir=$(mktemp -d); \
        rpm --import file:///etc/pki/rpm-gpg/RPM-GPG-KEY-amazon-linux-2023; \
        for suffix in -headless '' -devel -jmods; do \
          rpm -q "$package$suffix" >/dev/null; \
          filename="$package$suffix-$rpm_nvr.$rpm_release.$rpm_arch.rpm"; \
          curl --fail --silent --show-error --location --retry 3 -o "$rpm_dir/$filename" \
            "https://corretto.aws/downloads/resources/$rpm_version/$filename"; \
          rpm -K "$rpm_dir/$filename" | grep -F 'digests signatures OK'; \
        done; \
        dnf install -y --setopt=localpkg_gpgcheck=1 "$rpm_dir"/*.rpm; \
        rm -rf "$rpm_dir"; \
        rm -f "$JAVA_HOME/lib/src.zip"; \
        dnf clean all; \
        ;; \
      *) printf "Unsupported Corretto image OS %s:%s\n" "$ID" "$VERSION_ID" >&2; exit 1 ;; \
    esac
EOF
      ;;
    *) die "Unsupported JDK vendor $VENDOR" ;;
  esac

  docker build --pull --provenance=false --platform "linux/$ARCH" --tag "$1" "${args[@]}" "$context"
)

check_runtime() {
  local target=$1 source=$2 actual expected changed_keys
  actual=$(docker image inspect "$target" --format '{{json .Config}}' | image_config)
  expected=$(docker image inspect "$source" --format '{{json .Config}}' | image_config)
  if [[ "$actual" != "$expected" ]]; then
    changed_keys=$(jq -nr --argjson actual "$actual" --argjson expected "$expected" '
      $expected | keys[] as $key | select($actual[$key] != $expected[$key]) | $key')
    die "Image configuration changed for $target: $changed_keys"
  fi
  docker run --rm --network none --platform "linux/$ARCH" --entrypoint sh "$target" -ec '
    java_output=$("$JAVA_HOME/bin/java" -version 2>&1)
    printf "%s\n" "$java_output"
    java_version=$(printf "%s\n" "$java_output" | awk -F\" "/^openjdk version / { print \$2; exit }")
    test "$java_version" = "$1"
    if [ -n "$2" ]; then
      package="java-$3-amazon-corretto"
      . /etc/os-release
      case "$ID:$VERSION_ID" in
        debian:*)
          version=$(dpkg-query -W -f="\${Version}" "$package-jdk")
          test "$version" = "$2"
          ;;
        amzn:2023)
          expected="$2.amzn2023"
          if [ "$3" != 11 ]; then expected="$expected.1"; fi
          for suffix in -headless "" -devel -jmods; do
            version=$(rpm -q --qf "%{EPOCHNUM}:%{VERSION}-%{RELEASE}" "$package$suffix")
            test "$version" = "$expected"
          done
          ;;
        *) printf "Unsupported Corretto image OS %s:%s\n" "$ID" "$VERSION_ID" >&2; exit 1 ;;
      esac
    fi
  ' sh "$MINIMUM_VERSION" "$CORRETTO_PACKAGE_VERSION" "$MAJOR"
  timeout --kill-after=5s 120s docker run --rm --network none --platform "linux/$ARCH" \
    -e JAVA_OPTS='-Xms64m -Xmx512m -Dpinot.admin.system.exit=true' "$target" -help
}

push_digest() {
  local ref=$1 output digest
  output=$(docker push "$ref")
  printf '%s\n' "$output" >&2
  digest=$(awk '/digest: sha256:/ { for (i=1; i<NF; i++) if ($i == "digest:") {print $(i+1); exit} }' <<< "$output")
  [[ "$digest" =~ $digest_pattern ]] || die "Missing pushed digest for $ref"
  printf '%s\n' "$digest"
}

patch() {
  [[ "$ARCH" == amd64 || "$ARCH" == arm64 ]] || die "Unsupported architecture $ARCH"
  [[ "$OLD_DIGEST" =~ $digest_pattern ]] || die 'Invalid source digest'
  [[ "$PRIMARY_TAG" == "$RELEASE-$MAJOR-$VENDOR-linux-$ARCH" ]] || die "Unexpected platform tag $PRIMARY_TAG"
  local target="$image:$PRIMARY_TAG" source="$image@$OLD_DIGEST" original digest current alias
  original=$(config_of "$OLD_DIGEST")
  jq -e --arg arch "$ARCH" '.os == "linux" and .architecture == $arch' <<< "$original" >/dev/null
  docker pull --platform "linux/$ARCH" "$source"

  local -a aliases
  read -r -a aliases <<< "$PLATFORM_ALIASES"
  for alias in "${aliases[@]}"; do
    [[ "$alias" == "release-$PRIMARY_TAG" ]] || die "Unexpected platform alias $alias"
  done

  current=$(digest_of "$PRIMARY_TAG")
  if [[ "$current" == "$OLD_DIGEST" ]]; then
    build_patch "$target"
    check_runtime "$target" "$source"
    [[ "$(digest_of "$PRIMARY_TAG")" == "$OLD_DIGEST" ]] || die "Platform tag changed during build"
    digest=$(push_digest "$target")
  else
    # A retry can reuse a published replacement after checking its ancestry and rerunning the smoke tests.
    [[ "$current" =~ $digest_pattern ]] || die 'Invalid current platform digest'
    config_of "$current" | jq -e --argjson old "$original" '
      .os == $old.os and .architecture == $old.architecture and
      .rootfs.diff_ids[:($old.rootfs.diff_ids | length)] == $old.rootfs.diff_ids
    ' >/dev/null || die "Unexpected image already published at $target"
    target="$image@$current"
    docker pull --platform "linux/$ARCH" "$target"
    check_runtime "$target" "$source"
    digest=$current
  fi

  [[ "$digest" != "$OLD_DIGEST" ]] || die "Expected a patched digest for $PRIMARY_TAG"
  [[ "$(digest_of "$PRIMARY_TAG")" == "$digest" ]] || die "Published platform digest does not match the push"
  for alias in "${aliases[@]}"; do
    current=$(digest_of "$alias")
    [[ "$current" != "$digest" ]] || continue
    [[ "$current" == "$OLD_DIGEST" ]] || die "Unexpected image already published at $alias"
    docker tag "$target" "$image:$alias"
    [[ "$(push_digest "$image:$alias")" == "$digest" ]] || die "Platform alias digest does not match the push"
  done

  mkdir -p "$RESULTS_DIR"
  jq -n --arg tag "$PRIMARY_TAG" --arg old "$OLD_DIGEST" --arg digest "$digest" \
    '{tag: $tag, old: $old, digest: $digest}' > "$RESULTS_DIR/$PRIMARY_TAG.json"
}

resolve_platform() {
  local tag=$1 expected_arch=$2 original_digest=$3 digest
  [[ "$tag" == "$RELEASE-$MAJOR-$VENDOR-linux-$expected_arch" ]] || die "Unexpected platform tag $tag"
  [[ "$original_digest" =~ $digest_pattern ]] || die 'Invalid original platform digest'
  digest=$(jq -er --arg tag "$tag" --arg old "$original_digest" \
    'select(.tag == $tag and .old == $old) | .digest' "$RESULTS_DIR/$tag.json")
  [[ "$digest" =~ $digest_pattern && "$digest" != "$original_digest" ]] || die "Unpatched platform tag $tag"
  [[ "$(digest_of "$tag")" == "$digest" ]] || die "Platform tag $tag does not match tested digest"
  config_of "$digest" |
    jq -e --arg arch "$expected_arch" '.os == "linux" and .architecture == $arch' >/dev/null
  printf '%s\n' "$digest"
}

index_matches() {
  jq -e --arg amd64 "$1" --arg arm64 "$2" '
    [.manifests[] | [.platform.os, .platform.architecture, .digest]] | sort ==
    [["linux", "amd64", $amd64], ["linux", "arm64", $arm64]]
  ' >/dev/null
}

publish() {
  [[ "$OLD_INDEX_DIGEST" =~ $digest_pattern ]] || die 'Invalid original index digest'
  local amd64 arm64 alias remote group="$RELEASE-$MAJOR-$VENDOR"
  amd64=$(resolve_platform "$AMD64_TAG" amd64 "$AMD64_OLD_DIGEST")
  arm64=$(resolve_platform "$ARM64_TAG" arm64 "$ARM64_OLD_DIGEST")
  local -a aliases tags=()
  read -r -a aliases <<< "$INDEX_ALIASES"
  [[ "${#aliases[@]}" -gt 0 ]] || die 'Missing index aliases'
  for alias in "${aliases[@]}"; do
    case "$alias" in
      "$group"|"release-$group") ;;
      "$RELEASE"|"release-$RELEASE")
        [[ "$MAJOR" == 21 && "$VENDOR" == ms-openjdk ]] || die "Unexpected default alias $alias"
        ;;
      *) die "Unexpected index alias $alias" ;;
    esac
    remote=$(manifest "$alias")
    if index_matches "$amd64" "$arm64" <<< "$remote"; then
      continue
    fi
    [[ "$(jq -er '.digest' <<< "$remote")" == "$OLD_INDEX_DIGEST" ]] || die "Unexpected image already published at $alias"
    tags+=(--tag "$image:$alias")
  done

  if [[ "${#tags[@]}" -gt 0 ]]; then
    docker buildx imagetools create "${tags[@]}" "$image@$amd64" "$image@$arm64"
  fi
  for alias in "${aliases[@]}"; do
    manifest "$alias" | index_matches "$amd64" "$arm64"
  done
}

case "$RELEASE" in
  1.5.0|1.5.1) ;;
  *) die "Unsupported legacy release $RELEASE" ;;
esac
case "${1:-}" in
  patch) patch ;;
  publish) publish ;;
  *) die "Usage: $0 patch|publish" ;;
esac
