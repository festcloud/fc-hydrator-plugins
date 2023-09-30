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

package io.cdap.plugin.connector;

import io.cdap.cdap.api.annotation.Category;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.connector.BrowseDetail;
import io.cdap.cdap.etl.api.connector.BrowseEntity;
import io.cdap.cdap.etl.api.connector.BrowseEntityPropertyValue;
import io.cdap.cdap.etl.api.connector.BrowseRequest;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.cdap.etl.api.connector.ConnectorContext;
import io.cdap.cdap.etl.api.connector.ConnectorSpec;
import io.cdap.cdap.etl.api.connector.ConnectorSpecRequest;
import io.cdap.cdap.etl.api.connector.PluginSpec;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.cdap.plugin.common.Neo4jConstants.DATABASE;
import static io.cdap.plugin.common.Neo4jConstants.PASSWORD;
import static io.cdap.plugin.common.Neo4jConstants.URL;
import static io.cdap.plugin.common.Neo4jConstants.USER;

/**
 * Neo4j CDAP Connector
 */
@Plugin(type = Connector.PLUGIN_TYPE)
@Name(Neo4jConnector.NAME)
@Category("Database")
@Description("Connection to access data in Neo4j.")
public class Neo4jConnector implements Connector {
    private static final Logger LOG = LoggerFactory.getLogger(Neo4jConnector.class);
    public static final String NAME = "Neo4j";
    private static final String QUERY_NODES_COUNT = "MATCH (n) RETURN count(*)";
    private static final String QUERY_LABELS_COUNT = "MATCH (n) RETURN DISTINCT labels(n) as label, count(*) as count";

    private final Neo4jConnectorConfig config;
    private final Driver driver;

    public Neo4jConnector(Neo4jConnectorConfig config) {
        LOG.info("Init constructor");
        this.config = config;
        driver = GraphDatabase.driver(config.getUrl(), AuthTokens.basic(config.getUser(), config.getPassword()));
        LOG.info("Driver initialized");
    }

    @Override
    public void test(ConnectorContext context) throws ValidationException {
        LOG.info("Call test function");
        SessionConfig sessionConfig = SessionConfig.builder().withDatabase(config.getDatabase()).build();
        try (Session session = driver.session(sessionConfig)) {
            Result run = session.run(QUERY_NODES_COUNT);
            if (run.hasNext()) {
                LOG.info("Test connection passed successfully");
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            throw new ValidationException(Collections.singletonList(new ValidationFailure(e.getMessage())));
        }
    }

    @Override
    public BrowseDetail browse(ConnectorContext context, BrowseRequest request) throws IOException {
        LOG.info("Call browse function");
        BrowseDetail.Builder builder = BrowseDetail.builder();
        try (Session session = driver.session()) {
            List<Record> records = session.readTransaction(tx -> tx.run(QUERY_LABELS_COUNT).list());
            LOG.info("Queried {} records", records.size());
            records.forEach(record -> {
                BrowseEntity.Builder entityBuilder = BrowseEntity.builder("preview", "preview", "Noe4j");
                record.asMap().forEach((key, value) -> {
                    entityBuilder.canSample(true)
                            .addProperty(key, BrowseEntityPropertyValue.builder(value.toString(),
                                    BrowseEntityPropertyValue.PropertyType.STRING).build());
                });
                builder.addEntity(entityBuilder.build());
            });
            builder.setTotalCount(records.size());
        }
        return builder.build();
    }

    @Override
    public ConnectorSpec generateSpec(ConnectorContext context, ConnectorSpecRequest path) throws IOException {
        LOG.info("GenericSpec function");
        Map<String, String> properties = new HashMap<>();
        properties.put(URL, config.getUrl());
        properties.put(USER, config.getUser());
        properties.put(PASSWORD, config.getPassword());
        properties.put(DATABASE, config.getDatabase());
        return ConnectorSpec.builder()
                .addRelatedPlugin(new PluginSpec(NAME, BatchSink.PLUGIN_TYPE, properties))
                .build();

    }
}
