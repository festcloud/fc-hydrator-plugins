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
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.ConfigUtil;
import io.cdap.plugin.common.IdUtils;
import io.cdap.plugin.common.ReferencePluginConfig;

import javax.annotation.Nullable;

import static io.cdap.plugin.common.Neo4jConstants.DATABASE;
import static io.cdap.plugin.common.Neo4jConstants.PASSWORD;
import static io.cdap.plugin.common.Neo4jConstants.URL;
import static io.cdap.plugin.common.Neo4jConstants.USER;

/**
 * Neo4j CDAP Sink config
 */
public class Neo4jSinkConfig extends ReferencePluginConfig {

    @Name(URL)
    @Description("Neo4j URL")
    @Macro
    private String url;

    @Name(DATABASE)
    @Description("Database name to connect to")
    @Macro
    private String database;

    @Name(USER)
    @Description("Neo4j User")
    @Macro
    private String user;

    @Name(PASSWORD)
    @Description("Neo4j Password")
    @Macro
    private String password;

//  @Name(ConfigUtil.NAME_CONNECTION)
//  @Macro
//  @Nullable
//  @Description("The existing connection to use.")
//  private Neo4jConnectorConfig connection;

    @Name(ConfigUtil.NAME_USE_CONNECTION)
    @Nullable
    @Description("Whether to use an existing connection.")
    private Boolean useConnection;

    public Neo4jSinkConfig(String referenceName) {
        super(referenceName);
    }

    public void validate(FailureCollector collector) {
        IdUtils.validateReferenceName(referenceName, collector);
//    if (Strings.isNullOrEmpty(rowField)) {
//      collector.addFailure("Row field must be given as a property.", null).withConfigProperty(NAME_ROWFIELD);
//    }
    }

    public String getReferenceName() {
        return referenceName;
    }

    public String getUrl() {
        return url;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public String getDatabase() {
        return database;
    }
}
