#!/bin/bash -x
#
# Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# ThirdEye related changes
release_opts=
if [ -n "$RELEASE_VERSION" ]; then
release_opts="$release_opts -DreleaseVersion=$RELEASE_VERSION"
fi
if [ -n "$NEXT_VERSION" ]; then
release_opts="$release_opts -DdevelopmentVersion=$NEXT_VERSION"
fi
mvn -B release:clean release:prepare release:perform -Darguments="-DskipTests" $release_opts
