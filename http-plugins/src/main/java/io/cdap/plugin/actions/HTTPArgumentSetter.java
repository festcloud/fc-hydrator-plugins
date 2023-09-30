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

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.cdap.etl.api.action.ActionContext;
import io.cdap.plugin.http.HttpClient;
import io.cdap.plugin.http.HttpResponse;
import io.cdap.plugin.proto.Argument;
import io.cdap.plugin.proto.Configuration;
import java.io.IOException;
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

  private static final Gson gson = new Gson();


  protected final HTTPArgumentSetterConfig config;
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
  }

  @Override
  public void run(ActionContext context) throws Exception {
    FailureCollector collector = context.getFailureCollector();
    config.validate(collector);
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
//    RetryableErrorHandling errorHandlingStrategy = httpErrorHandler.getErrorHandlingStrategy(httpStatusCode);
  }

  //  @Override
  protected void handleResponse(ActionContext context, String body) throws IOException {
    try {
      Configuration configuration = gson.fromJson(body, Configuration.class);
      for (Argument argument : configuration.getArguments()) {
        String name = argument.getName();
        String value = argument.getValue();
        if (value != null) {
          context.getArguments().set(name, value);
        } else {
          throw new RuntimeException(
              "Configuration '" + name + "' is null. Cannot set argument to null.");
        }
      }
    } catch (JsonSyntaxException e) {
      throw new RuntimeException(String.format("Could not parse response from '%s': %s",
          config.getUrl(), e.getMessage()));
    }
  }

  public HttpClient getHttpClient() {
    if (httpClient == null) {
      httpClient = new HttpClient(config);
    }
    return httpClient;
  }


}
