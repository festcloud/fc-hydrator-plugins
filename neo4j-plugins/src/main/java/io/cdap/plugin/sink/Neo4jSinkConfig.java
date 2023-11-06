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

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.ConfigUtil;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.common.IdUtils;
import io.cdap.plugin.connector.Neo4jConnectorConfig;
import io.cdap.plugin.sink.objects.RelationDto;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.cdap.plugin.common.Neo4jConstants.DATABASE;
import static io.cdap.plugin.common.Neo4jConstants.NODE_LABEL;
import static io.cdap.plugin.common.Neo4jConstants.RELATIONS;

/**
 * Neo4j CDAP Sink config
 */
public class Neo4jSinkConfig extends PluginConfig {
    @Name(Constants.Reference.REFERENCE_NAME)
    @Description(Constants.Reference.REFERENCE_NAME_DESCRIPTION)
    public String referenceName;

    @Name(DATABASE)
    @Description("Database name to connect to")
    @Macro
    private String database;

    @Name(ConfigUtil.NAME_CONNECTION)
    @Macro
    @Nullable
    @Description("The connection to use.")
    private Neo4jConnectorConfig connection;

    @Name(ConfigUtil.NAME_USE_CONNECTION)
    @Nullable
    @Description("Whether to use an existing connection.")
    private Boolean useConnection;

    @Name(RELATIONS)
    @Macro
    @Nullable
    @Description("Aggregates to compute on grouped records. ")
    private String relations;

    @Name(NODE_LABEL)
    @Macro
    @Description("Label that has to be assigned to particular store")
    private String nodeLabel;

    public void validate(FailureCollector collector, List<String> allowedElements) {
        IdUtils.validateReferenceName(referenceName, collector);
        ConfigUtil.validateConnection(this, useConnection, connection, collector);
        validateRelations(allowedElements);
    }

    private void validateRelations(List<String> allowedElements) {
        // TODO: add implementation, separate responsibility in the method below
    }

    public List<RelationDto> getRelations() {
        if (containsMacro(RELATIONS) || Strings.isNullOrEmpty(relations)) {
            return null;
        }
        List<RelationDto> result = new ArrayList<>();
        Set<String> relationsName = new HashSet<>();
        String regex = "^(.+):([><])\\((.+)\\)$";
        Pattern pattern = Pattern.compile(regex);
        for (String relationStr : Splitter.on(',').trimResults().split(relations)) {
            Matcher matcher = pattern.matcher(relationStr);
            if (matcher.matches() && matcher.groupCount() == 3) {


                String elementName = matcher.group(3);
                String direction = matcher.group(2);
                String belongs = matcher.group(1);

                /*if (!allowedElements.contains(elementName)) {
                    throw new IllegalArgumentException(
                            String.format("Cannot create relations with not allowed Element Name '%s'.", elementName));
                }*/

                if (relationsName.contains(elementName)) {
                    throw new IllegalArgumentException(
                            String.format("Cannot create relations with the same name '%s'.", elementName));
                }

                relationsName.add(elementName);
                RelationDto relationDto = RelationDto.RelationDtoBuilder.aRelationObj()
                        .withElementName(elementName)
                        .withDirection(direction)
                        .withRelationName(belongs)
                        .build();
                result.add(relationDto);
            } else {
                throw new IllegalArgumentException(
                        String.format("Cannot create relations with the same name '%s'.", relationStr));

            }
        }
        return result;
    }

    public String getReferenceName() {
        return referenceName;
    }

    public String getUrl() {
        return connection.getUrl();
    }

    public String getUser() {
        return connection.getUser();
    }

    public String getPassword() {
        return connection.getPassword();
    }

    public String getDatabase() {
        return database;
    }

    public String getNodeLabel() {
        return nodeLabel;
    }
}
