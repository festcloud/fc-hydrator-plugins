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

package io.cdap.plugin.sink;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.plugin.common.Constants;

import static io.cdap.plugin.common.Neo4jConstants.PASSWORD;
import static io.cdap.plugin.common.Neo4jConstants.URL;
import static io.cdap.plugin.common.Neo4jConstants.USER;

/**
 * Neo4j CDAP Sink config
 */
public class Neo4jSinkConfig extends PluginConfig {
    @Name(Constants.Reference.REFERENCE_NAME)
    @Description(Constants.Reference.REFERENCE_NAME_DESCRIPTION)
    private final String referenceName;
    @Name(URL)
    @Description("Neo4j URL")
    @Macro
    private final String neo4jUrl;

    @Name(USER)
    @Description("Neo4j User")
    @Macro
    private final String neo4jUser;

    @Name(PASSWORD)
    @Description("Neo4j Password")
    @Macro
    private final String neo4jPassword;

    public Neo4jSinkConfig(String referenceName, String neo4jUrl, String neo4jUser, String neo4jPassword) {
        this.referenceName = referenceName;
        this.neo4jUrl = neo4jUrl;
        this.neo4jUser = neo4jUser;
        this.neo4jPassword = neo4jPassword;
    }

    public String getReferenceName() {
        return referenceName;
    }

    public String getNeo4jUrl() {
        return neo4jUrl;
    }

    public String getNeo4jUser() {
        return neo4jUser;
    }

    public String getNeo4jPassword() {
        return neo4jPassword;
    }
}
