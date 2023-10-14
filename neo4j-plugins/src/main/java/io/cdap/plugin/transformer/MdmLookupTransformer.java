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
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.InvalidEntry;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.TransformContext;
import io.cdap.plugin.common.ConfigUtil;
import io.cdap.plugin.connector.Neo4jConnectorConfig;
import io.cdap.plugin.sink.Neo4jDataService;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static io.cdap.plugin.common.Neo4jConstants.COLUM_NAME;
import static io.cdap.plugin.common.Neo4jConstants.DATABASE;
import static io.cdap.plugin.common.Neo4jConstants.SCHEMA;

/**
 * MdmLookup plugin to verify if data present in MDM.
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name("MdmLookup")
@Description("Verify whether the requested value is present in MDM.")
public class MdmLookupTransformer extends Transform<StructuredRecord, StructuredRecord> {

    private static final Logger LOG = LoggerFactory.getLogger(MdmLookupTransformer.class);
    private static final Integer ERROR_CODE = 42;

    private final LookupConfig config;
    private Neo4jDataService dataService;
    private String lookupColumn;
    private Driver driver;
    private Schema outSchema;


    public MdmLookupTransformer(LookupConfig config) {
        this.config = config;
    }

    @Override
    public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) {
        Objects.requireNonNull(input.getSchema());
        Objects.requireNonNull(input.getSchema().getFields());

        List<String> missedIds = input.getSchema().getFields().stream()
                .flatMap(field -> {
                    List<String> ids = new ArrayList<>();
                    if (Schema.Type.ARRAY.equals(field.getSchema().getType())) {
                        for (Object inner : ((Collection) Objects.requireNonNull(input.get(field.getName())))) {
                            if (inner instanceof StructuredRecord) {
                                ids.add(getIdValue((StructuredRecord) inner, field));
                            }
                        }
                    } else if (Schema.Type.RECORD.equals(field.getSchema().getType())) {
                        ids.add(getIdValue(input, field));
                    } else if (Schema.Type.UNION.equals(field.getSchema().getType())) {
                        if (Schema.Type.RECORD.equals(field.getSchema().getNonNullable().getType())) {
                            ids.add(getIdValue(input, field));
                        }
                    }
                    return ids.stream();
                })
                .filter(Objects::nonNull)
                .filter(uid -> dataService.getUniqueNodeByProperty(lookupColumn, uid) == null)
                .peek(uid -> {
                    LOG.info("Integrity validation failed for: {}", uid);
                    emitter.emitError(new InvalidEntry<>(ERROR_CODE, uid, input));
                })
                .collect(Collectors.toList());

        if (missedIds.isEmpty()) {
            LOG.info("Integrity validation passed");
            emitter.emit(input);
        }
    }

    private String getIdValue(StructuredRecord input, Schema.Field field) {
        Object fieldValue = input.get(field.getName());
        StructuredRecord structuredFieldValue = (StructuredRecord) fieldValue;
        if (structuredFieldValue != null) {
            return structuredFieldValue.get(lookupColumn);
        } else {
            return null;
        }
    }

    @Override
    public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
        super.configurePipeline(pipelineConfigurer);
        Schema schema = pipelineConfigurer.getStageConfigurer().getInputSchema();
        pipelineConfigurer.getStageConfigurer().setOutputSchema(schema);
    }

    @Override
    public void initialize(TransformContext context) throws Exception {
        super.initialize(context);
        lookupColumn = config.getColumName();
        LOG.info("Init connection to MDM database");
        driver = GraphDatabase.driver(config.getDatabaseURI(),
            AuthTokens.basic(config.getUserName(), config.getUserPassword()));
        SessionConfig sessionConfig = SessionConfig.builder().withDatabase(config.getDatabase()).build();
        Session session = driver.session(sessionConfig);
        dataService = new Neo4jDataService(session);
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

        @Name(ConfigUtil.NAME_CONNECTION)
        @Macro
        @Nullable
        @Description("The connection to use.")
        private Neo4jConnectorConfig connection;

        @Name(ConfigUtil.NAME_USE_CONNECTION)
        @Nullable
        @Description("Whether to use an existing connection.")
        private Boolean useConnection;

        @Name(COLUM_NAME)
        @Description("Any uniq identifier you would like to validate in MDM")
        @Macro
        private String columName;

        @Name(DATABASE)
        @Description("Database name to connect to")
        @Macro
        private String database;

        @Name(SCHEMA)
        @Description("Output schema")
        @Macro
        private String schema;

        public void validate(FailureCollector collector) {
            ConfigUtil.validateConnection(this, useConnection, connection, collector);
        }

        public String getUserName() {
            return connection.getUser();
        }

        public String getUserPassword() {
            return connection.getPassword();
        }

        public String getDatabaseURI() {
            return connection.getUrl();
        }

        public String getColumName() {
            return columName;
        }

        public String getDatabase() {
            return database;
        }
    }

}

