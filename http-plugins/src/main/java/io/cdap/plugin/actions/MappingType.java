/*
 * Copyright © 2022 Cask Data, Inc.
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

import io.cdap.plugin.http.exceptions.InvalidPropertyTypeException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public enum MappingType {
  STANDARD("none"),
  CUSTOM_MAPPING("customMapping");

  private final String value;

  MappingType(String value) {
    this.value = value;
  }

  /**
   * Returns the MappingType.
   *
   * @param value the value is string type.
   * @return The AuthType
   */
  public static MappingType fromValue(String value) {
    return Arrays.stream(MappingType.values()).filter(authtype -> authtype.getValue().equals(value))
        .findAny().orElseThrow(() -> new InvalidPropertyTypeException(
            HTTPArgumentSetterConfig.PROPERTY_MAPPING_TYPE_LABEL,
            value, getAllowedValues()));
  }

  public static List<String> getAllowedValues() {
    return Arrays.stream(MappingType.values()).map(v -> v.getValue())
        .collect(Collectors.toList());
  }

  public String getValue() {
    return value;
  }
}
