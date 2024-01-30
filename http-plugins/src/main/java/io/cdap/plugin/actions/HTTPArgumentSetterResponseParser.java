/*
 * Copyright © 2017-2019 Cask Data, Inc.
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
import com.jayway.jsonpath.JsonPath;
import io.cdap.cdap.etl.api.action.ActionContext;
import io.cdap.plugin.proto.Argument;
import io.cdap.plugin.proto.Configuration;
import java.util.Map;
import java.util.stream.Collectors;
import net.minidev.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HTTPArgumentSetterResponseParser {

  private static final Logger LOG = LoggerFactory.getLogger(HTTPArgumentSetter.class);

  private static final Gson gson = new Gson();

  public static void jsonPathParser(ActionContext context, String body,
      Map<String, String> mapping) {
    Object document = com.jayway.jsonpath.Configuration.defaultConfiguration().jsonProvider()
        .parse(body);
    mapping.forEach((propertyName, path) -> {
      Object value = JsonPath.read(document, path);
      if (value instanceof JSONArray) {
        JSONArray jsonArray = ((JSONArray) value);
        if (jsonArray.size() == 1) {
          value = jsonArray.get(0);
        } else {
          value = jsonArray.stream()
              .map(String::valueOf)
              .collect(Collectors.joining(","));
        }
      }
      LOG.debug("Add new variable to pipeline {} - \"{}\"", propertyName, value);
      context.getArguments().set(propertyName, String.valueOf(value));
    });
  }

  public static void standardParser(ActionContext context, String body,
      HTTPArgumentSetterConfig config) {
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


}
