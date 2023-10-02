/*
 * Copyright © 2015-2019 Cask Data, Inc.
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
package io.cdap.plugin.transformer;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.InvalidEntry;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.TransformContext;
import io.cdap.plugin.sink.Neo4jDataService;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * MdmLookup plugin to verify if data present in MDM.
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name("MdmLookup")
@Description("Verify whether the requested value is present in MDM.")
public class MdmLookupTransformer extends Transform<StructuredRecord, StructuredRecord> {

    private static final Logger LOG = LoggerFactory.getLogger(MdmLookupTransformer.class);

    private final LookupConfig config;
    private Neo4jDataService dataService;
    private String lookupColumn;
    private Driver driver;

    public MdmLookupTransformer(LookupConfig config) {
        this.config = config;
    }

    @Override
    public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) {
        List<String> lookupData = new ArrayList<>();
        for (Schema.Field field : input.getSchema().getFields()) {
            processField(input, lookupData, field, false);
        }

        final List<String> notFoundList = lookupData.stream()
            .filter(uuid -> dataService.getUniqueNodeByProperty(lookupColumn, uuid) == null)
            .collect(Collectors.toList());

        if (!notFoundList.isEmpty()) {
            notFoundList.forEach(id -> emitter.emitError(
                new InvalidEntry<>(1, String.format("Not found in MDM. UUID: %s", id), input)));
        } else {
            emitter.emit(input);
        }
    }

    private void processField(StructuredRecord input, List<String> lookupData,
                              Schema.Field field, Boolean isChildRecord) {

        if (Schema.Type.ARRAY.equals(field.getSchema().getType())) {
            for (Schema.Field childField : field.getSchema().getComponentSchema().getFields()) {
                List<StructuredRecord> structuredRecordList = input.get(field.getName());
                processField(structuredRecordList.get(0), lookupData, childField, true);
            }
        } else if (field.getSchema().getType().isSimpleType() && isChildRecord) {
            LOG.info("It's simple type");
            if (field.getName().equals(lookupColumn)) {
                lookupData.add(processProperty(input, field));
            }
        } else {
            LOG.warn("Unrecognized field type");
        }
    }

    private String processProperty(StructuredRecord input, Schema.Field field) {
        return input.get(field.getName());
    }

    @Override
    public void initialize(TransformContext context) throws Exception {
        super.initialize(context);
        lookupColumn = config.getColumName();
        LOG.info("Init connection to MDM database");
        driver = GraphDatabase.driver(config.getDatabaseURI(),
            AuthTokens.basic(config.getUserName(), config.getUserPassword()));
        dataService = new Neo4jDataService(driver.session());
    }

    /**
     * Destroy plugin and close Neo4j connection.
     */
    @Override
    public void destroy() {
        LOG.info("Close connection to MDM database.");
        driver.close();
        super.destroy();
    }

    /**
     * MDM Lookup configuration class.
     */
    public static class LookupConfig extends PluginConfig {

        @Name("userName")
        @Description("User name for Neo4J database.")
        @Macro
        private final String userName;

        @Name("userPassword")
        @Description("User password for Neo4J database.")
        @Macro
        private final String userPassword;

        @Name("databaseHost")
        @Description("<server_host>:<port>")
        @Macro
        private final String databaseURI;

        @Name("columName")
        @Description("Any uniq identifier you would like to validate in MDM")
        @Macro
        private final String columName;

        public LookupConfig(String userName, String userPassword, String databaseHost, String columName) {
            this.userName = userName;
            this.userPassword = userPassword;
            this.databaseURI = databaseHost;
            this.columName = columName;
        }

        public String getUserName() {
            return userName;
        }

        public String getUserPassword() {
            return userPassword;
        }

        public String getDatabaseURI() {
            return databaseURI;
        }

        public String getColumName() {
            return columName;
        }
    }

}

