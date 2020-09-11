/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";

export function fetchNewRelease() {
    const context = useDocusaurusContext();
    const { siteConfig = {} } = context;
    const {
        metadata: { latest_release: latestRelease },
    } = siteConfig.customFields;
    const releaseDate = Date.parse(latestRelease.date);
    const releaseNow = new Date();
    const releaseDiffTime = Math.abs(releaseNow - releaseDate);
    const releaseDiffDays = Math.ceil(releaseDiffTime / (1000 * 60 * 60 * 24));

    let releaseViewedAt = null;

    if (typeof window !== "undefined") {
        releaseViewedAt = new Date(
            parseInt(window.localStorage.getItem("releaseViewedAt") || "0")
        );
    }

    if (
        releaseDiffDays < 30 &&
        (!releaseViewedAt || releaseViewedAt < releaseDate)
    ) {
        return latestRelease;
    }

    return null;
}

export function viewedNewRelease() {
    if (typeof window !== "undefined") {
        window.localStorage.setItem("releaseViewedAt", new Date().getTime());
    }
}

export default { fetchNewRelease, viewedNewRelease };
