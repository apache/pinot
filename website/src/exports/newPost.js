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

export function fetchNewPost() {
    const context = useDocusaurusContext();
    const { siteConfig = {} } = context;
    const {
        metadata: { latest_post: latestPost },
    } = siteConfig.customFields;
    const date = Date.parse(latestPost.date);
    const now = new Date();
    const diffTime = Math.abs(now - date);
    const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));

    let viewedAt = null;

    if (typeof window !== "undefined") {
        viewedAt = new Date(
            parseInt(window.localStorage.getItem("blogViewedAt") || "0")
        );
    }

    if (diffDays < 30 && (!viewedAt || viewedAt < date)) {
        return latestPost;
    }

    return null;
}

export function viewedNewPost() {
    if (typeof window !== "undefined") {
        window.localStorage.setItem("blogViewedAt", new Date().getTime());
    }
}

export default { fetchNewPost, viewedNewPost };
