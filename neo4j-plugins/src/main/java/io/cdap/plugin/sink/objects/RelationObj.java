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

public class RelationObj {
  private String elementName;
  private String direction;
  private String belongs;

  public String getElementName() {
    return elementName;
  }

  public String getDirection() {
    return direction;
  }

  public String getBelongs() {
    return belongs;
  }

  public static final class RelationObjBuilder {

    private String elementName;
    private String direction;
    private String belongs;

    private RelationObjBuilder() {
    }

    public static RelationObjBuilder aRelationObj() {
      return new RelationObjBuilder();
    }

    public RelationObjBuilder withElementName(String elementName) {
      this.elementName = elementName;
      return this;
    }

    public RelationObjBuilder withDirection(String direction) {
      this.direction = direction;
      return this;
    }

    public RelationObjBuilder withBelongs(String belongs) {
      this.belongs = belongs;
      return this;
    }

    public RelationObj build() {
      RelationObj relationObj = new RelationObj();
      relationObj.belongs = this.belongs;
      relationObj.direction = this.direction;
      relationObj.elementName = this.elementName;
      return relationObj;
    }
  }
}
