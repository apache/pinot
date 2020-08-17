<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# ThirdEye Documentation

This directory contains the documentation for ThirdEye. It is available online at https://thirdeye.readthedocs.io. 

### Building docs

The documentation is built using the [sphinx](https://www.sphinx-doc.org/) framework.

If you have python/pip, you can install the dependencies using the command below.
```
pip3 install -r requirements.txt
``` 

Build docs using
```
make html
```
The rendered html can be found at `_build/html/index.html`

### Updating docs
1. Edit or add files as needed.
2. Build using `make livedocs`.
3. Open `_build/html/index.html` in your favorite browser. The page should auto-update with your changes.
4. Ensure the contents and links work correctly
5. Submit a PR!


> NOTE: You may see some differences locally as the version of `sphinx-build` on your local host might not be the same as the one used in [readthedocs.io](https://readthedocs.io) for building the pinot website docs.