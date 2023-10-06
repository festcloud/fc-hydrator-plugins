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

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.validation.InvalidConfigPropertyException;
import io.cdap.plugin.common.http.HTTPConfig;
import io.cdap.plugin.http.AuthType;
import java.util.Set;
import javax.annotation.Nullable;
import javax.ws.rs.HttpMethod;

/**
 * Common http plugin properties.
 */
public class HTTPArgumentSetterConfig extends HTTPConfig {

  public static final Set<String> METHODS = ImmutableSet.of(HttpMethod.GET, HttpMethod.HEAD,
      HttpMethod.OPTIONS, HttpMethod.PUT, HttpMethod.POST, HttpMethod.DELETE);

  public static final String NAME_NUM_RETRIES = "numRetries";
  public static final String NAME_AUTH_TYPE = "authType";
  public static final String NAME_MAPPING_TYPE = "mappingType";
  public static final String NAME_USERNAME = "username";
  public static final String NAME_PASSWORD = "password";
  public static final String PROPERTY_AUTH_TYPE_LABEL = "Auth type";
  public static final String PROPERTY_MAPPING_TYPE_LABEL = "Mapping type";
  public static final String NAME_METHOD = "method";
  public static final String NAME_MAPPING = "mapping";


  @Nullable
  @Description("The number of times the request should be retried if the request fails. Defaults to 0.")
  @Macro
  private final Integer numRetries;

  @Nullable
  @Name(NAME_PASSWORD)
  @Description("Password for basic authentication.")
  @Macro
  protected String password;

  @Name(NAME_AUTH_TYPE)
  @Description("Type of authentication used to submit request. \n"
      + "OAuth2, Service account, Basic Authentication types are available.")
  private String authType;

  @Name(NAME_MAPPING_TYPE)
  @Description("Type of response parser used.")
  @Macro
  private String mappingType;

  @Nullable
  @Name(NAME_USERNAME)
  @Description("Username for basic authentication.")
  @Macro
  private String username;

  @Name(NAME_METHOD)
  @Description("The http request method.")
  @Macro
  private final String method;

  @Nullable
  @Description("The http request method. Defaults to GET.")
  @Macro
  private String body;

  @Name(NAME_MAPPING)
  @Description(
      "Maps complex JSON to output fields using JSON path expressions. First field defines the output "
          +
          "field name and the second field specifies the JSON path expression, such as '$.employee.name.first'. "
          +
          "See reference documentation for additional examples.")
  @Nullable
  private String mapping;


  public HTTPArgumentSetterConfig() {
    super();
    numRetries = 0;
    method = HttpMethod.GET;
  }

  public static void assertIsSet(Object propertyValue, String propertyName, String reason) {
    if (propertyValue == null) {
      throw new InvalidConfigPropertyException(
          String.format("Property '%s' must be set, since %s", propertyName, reason), propertyName);
    }
  }

  @SuppressWarnings("ConstantConditions")
  public void validate(FailureCollector collector) {
    super.validate(collector);
    validateMethod(collector);
    validateNumRetries(collector);
    validateAuthentication(collector);
    validateMapping(collector);
  }

  private void validateMethod(FailureCollector collector) {
    if (!containsMacro(NAME_METHOD) && !METHODS.contains(method.toUpperCase())) {
      collector.addFailure(String.format("Invalid request method '%s'.", method),
              String.format("Supported methods are : %s", Joiner.on(',').join(METHODS)))
          .withConfigProperty(NAME_METHOD);
    }
  }

  private void validateNumRetries(FailureCollector collector) {
    if (!containsMacro(NAME_NUM_RETRIES) && numRetries != null && numRetries < 0) {
      collector.addFailure(String.format("Invalid numRetries '%d'.", numRetries),
              "Retries must be a positive number or zero.")
          .withConfigProperty(NAME_NUM_RETRIES);
    }
  }

  private void validateAuthentication(FailureCollector collector) {
    AuthType authType = getAuthType();
    switch (authType) {
      case BASIC_AUTH:
        String reasonBasicAuth = "Basic Authentication is enabled";
        if (!containsMacro(NAME_USERNAME)) {
          assertIsSet(getUsername(), NAME_USERNAME, reasonBasicAuth);
        }
        if (!containsMacro(NAME_PASSWORD)) {
          assertIsSet(getPassword(), NAME_PASSWORD, reasonBasicAuth);
        }
        break;
    }
  }

  private void validateMapping(FailureCollector collector) {
    MappingType mappingType = getMappingType();
    switch (mappingType) {
      case STANDARD:
        // todo:: implement
        break;
      case CUSTOM_MAPPING:
        String reasonCustomMapping = "Custom mapping is selected";
        if (!containsMacro(NAME_MAPPING)) {
          assertIsSet(getMapping(), NAME_MAPPING, reasonCustomMapping);
        }
        break;
    }
  }

  public AuthType getAuthType() {
    return AuthType.fromValue(authType);
  }

  public MappingType getMappingType() {
    return MappingType.fromValue(mappingType);
  }

  public String getAuthTypeString() {
    return authType;
  }

  public String getMappingTypeString() {
    return mappingType;
  }

  @Nullable
  public String getMapping() {
    return mapping;
  }

  public String getMethod() {
    return method;
  }

  @Nullable
  public String getBody() {
    return body;
  }

  @Nullable
  public Integer getNumRetries() {
    return numRetries;
  }

  public String geMethod() {
    return method;
  }

  @Nullable
  public String getUsername() {
    return username;
  }

  @Nullable
  public String getPassword() {
    return password;
  }

}
