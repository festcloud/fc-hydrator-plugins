/*
 * Copyright © 2023 Cask Data, Inc.
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
package io.cdap.plugin.sink.objects;

public class RelationDto {
  private String elementName;
  private String direction;
  private String relationName;

  public String getElementName() {
    return elementName;
  }

  public String getDirection() {
    return direction;
  }

  public String getRelationName() {
    return relationName;
  }

  public static final class RelationDtoBuilder {

    private String elementName;
    private String direction;
    private String relationName;

    private RelationDtoBuilder() {
    }

    public static RelationDtoBuilder aRelationObj() {
      return new RelationDtoBuilder();
    }

    public RelationDtoBuilder withElementName(String elementName) {
      this.elementName = elementName;
      return this;
    }

    public RelationDtoBuilder withDirection(String direction) {
      this.direction = direction;
      return this;
    }

    public RelationDtoBuilder withRelationName(String relationName) {
      this.relationName = relationName;
      return this;
    }

    public RelationDto build() {
      RelationDto relationDto = new RelationDto();
      relationDto.relationName = this.relationName;
      relationDto.direction = this.direction;
      relationDto.elementName = this.elementName;
      return relationDto;
    }
  }

  @Override
  public String toString() {
    return "RelationDto{" +
            "elementName='" + elementName + '\'' +
            ", direction='" + direction + '\'' +
            ", relationName='" + relationName + '\'' +
            '}';
  }
}
