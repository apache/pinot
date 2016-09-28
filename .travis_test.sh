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

# Ignore changes not related to pinot code
git diff --name-only $TRAVIS_COMMIT_RANGE | egrep '^(pinot-|pom.xml|.travis|.codecov)'
if [ $? -ne 0 ]; then
  echo 'No changes related to the pinot code, skip the test.'
  exit 0
fi

# Only run tests for JDK 8
if [ $TRAVIS_JDK_VERSION != 'oraclejdk7' ]; then
  echo 'Skip tests for version other than oraclejdk7.'
  exit 0
fi

mvn test -B -P travis
if [ $? -ne 0 ]; then
  exit 1
fi

bash <(cat .codecov_bash)
