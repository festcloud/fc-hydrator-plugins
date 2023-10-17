/*
 * Copyright © 2016-2021 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.actions;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.cdap.etl.api.action.ActionContext;
import io.cdap.plugin.http.HttpClient;
import io.cdap.plugin.http.HttpResponse;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sets pipeline arguments for the run based on the contents of an http call.
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name("ArgumentSetter")
@Description("Argument setter for dynamically configuring pipeline.")
public class HTTPArgumentSetter extends Action {

  private static final Logger LOG = LoggerFactory.getLogger(HTTPArgumentSetter.class);


  protected final HTTPArgumentSetterConfig config;
  // Map of field name to path as specified in the configuration, if none specified then it's direct mapping.
  private final Map<String, String> mapping = Maps.newHashMap();
  private HttpClient httpClient;
  private int httpStatusCode;
  private HttpResponse response;

  public HTTPArgumentSetter(HTTPArgumentSetterConfig conf) {
    this.config = conf;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    FailureCollector collector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    config.validate(collector);
    extractMappings(collector);
  }

  @Override
  public void run(ActionContext context) throws Exception {
    FailureCollector collector = context.getFailureCollector();
    config.validate(collector);
    extractMappings(collector);
    collector.getOrThrowException();

    LOG.debug("Fetching '{}'", config.getUrl());

    response = new HttpResponse(getHttpClient().executeHTTP(config.getUrl()));
    httpStatusCode = response.getStatusCode();
    LOG.debug("Request to {} resulted in response code {}.", config.getUrl(), httpStatusCode);

    String body = response.getBody();

    int responseCode = response.getStatusCode();

    LOG.debug("Request to {} resulted in response code {}.", config.getUrl(), responseCode);
    if (responseCode != 200) {
      throw new IllegalStateException(
          String.format("Received non-200 response code %d. Response message = %s",
              responseCode, body));
    }
    handleResponse(context, body);
  }

  protected void handleResponse(ActionContext context, String body) {

    switch (config.getMappingType()) {
      case CUSTOM_MAPPING:
        HTTPArgumentSetterResponseParser.jsonPathParser(context, body, mapping);
        break;
      case STANDARD:
        HTTPArgumentSetterResponseParser.standardParser(context, body, config);
        break;
      case VOID:
        // do nothing
    }
  }

  private void extractMappings(FailureCollector collector) {
    MappingType mappingType = config.getMappingType();
    if (MappingType.STANDARD.equals(mappingType)) {
      LOG.debug("Use standard mapping, not parsing are needed.");
      return;
    }
    String[] pathMaps = config.getMapping().split(",");
    for (String pathMap : pathMaps) {
      String[] mapParts = pathMap.split(":");
      if (mapParts.length != 2 || Strings.isNullOrEmpty(mapParts[0]) || Strings.isNullOrEmpty(
          mapParts[1])) {
        collector.addFailure("Both field name and JSON expression map must be provided.", null)
            .withConfigElement(HTTPArgumentSetterConfig.NAME_MAPPING, pathMap);
      } else {
        mapping.put(mapParts[0], mapParts[1]);
      }
    }
  }

  public HttpClient getHttpClient() {
    if (httpClient == null) {
      httpClient = new HttpClient(config);
    }
    return httpClient;
  }


}
