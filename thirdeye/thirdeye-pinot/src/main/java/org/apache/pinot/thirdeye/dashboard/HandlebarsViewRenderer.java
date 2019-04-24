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

package org.apache.pinot.thirdeye.dashboard;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import javax.ws.rs.WebApplicationException;

import com.github.jknack.handlebars.Handlebars;
import com.github.jknack.handlebars.Template;
import com.github.jknack.handlebars.cache.GuavaTemplateCache;
import com.github.jknack.handlebars.io.TemplateSource;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import io.dropwizard.views.View;
import io.dropwizard.views.ViewRenderer;

/**
 * A {@link ViewRenderer} which renders Handlebars ({@code .hbs}) templates.
 */
public class HandlebarsViewRenderer implements ViewRenderer {
    /**
     * For use by Handlebars.java internally.
     */
    private static final Cache<TemplateSource, Template> templateCache = CacheBuilder
            .newBuilder()
            .build();

    private static final String HANDLEBARS_FILE_SUFFIX = ".hbs";

    /**
     * Handlebars.java does not cache reads of Template content from resources.
     */
    @VisibleForTesting
    static final LoadingCache<String, Template> compilationCache = CacheBuilder
            .newBuilder()
            .build(new CacheLoader<String, Template>() {
                @Override
                public Template load(String srcUrl) throws Exception {
                    return HANDLEBARS.compile(srcUrl.replaceAll(HANDLEBARS_FILE_SUFFIX + "$", ""));
                }
            });

    /**
     * Exposed for use in {@link HandlebarsHelperBundle} for miscellaneous configuration.
     */
    static final Handlebars HANDLEBARS = new Handlebars()
            .with(new GuavaTemplateCache(templateCache));

    public HandlebarsViewRenderer() {
    }

    @Override
    public boolean isRenderable(View view) {
        return view.getTemplateName().endsWith(HANDLEBARS_FILE_SUFFIX);
//                || view.getTemplateName().endsWith(".mustache"); // we can replace dropwizard-views-mustache with this.
    }

    @Override
    public void render(View view, Locale locale, OutputStream output) throws IOException, WebApplicationException {
        try (Writer writer = new OutputStreamWriter(output, view.getCharset().orElse(Charsets.UTF_8))) {
            compilationCache.get(view.getTemplateName()).apply(view, writer);
        } catch (FileNotFoundException | ExecutionException e) {
            throw new FileNotFoundException("Template " + view.getTemplateName() + " not found.");
        }
    }

    @Override
    public void configure(Map<String, String> options) {
      // TODO Auto-generated method stub
      // jteoh - new method required as part of dropwizard upgrade to 0.8.2
    }

    @Override
    public String getConfigurationKey() {
        return "handlebars";
    }
}