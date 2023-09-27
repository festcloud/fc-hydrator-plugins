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

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.Value;
import org.neo4j.driver.types.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Neo4j specific data service
 */
public class Neo4jDataService {
    private static final Logger LOG = LoggerFactory.getLogger(Neo4jDataService.class);

    private static final List<Schema.Type> NUMERIC_TYPES = Arrays.asList(Schema.Type.INT, Schema.Type.LONG,
            Schema.Type.FLOAT, Schema.Type.DOUBLE);
    private final Session session;

    public Neo4jDataService(Session session) {
        this.session = session;
    }

    /**
     * MATCH (element {id: '3de87cb7-b7fb-11ea-82ff-97299e06153c'})
     * RETURN element
     *
     * @param propertyName  unique identifier property name
     * @param propertyValue unique identifier value
     * @return Node by unique identifier or null if no Node found or if more then one Node was returned
     */
    public Node getUniqueNodeByProperty(String propertyName, String propertyValue) {
        Result result = session.run("MATCH (n {" + propertyName + ": '" + propertyValue + "'}) RETURN n");
        if (result.hasNext()) {
            if (result.list().size() > 1) {
                LOG.warn("Returned more than one result");
                return null;
            }
            Value value = result.single().get(0);
            return value.asNode();
        }
        return null;
    }

    /**
     * MATCH (element {id: '3de87cb7-b7fb-11ea-82ff-97299e06153c'})
     * ON MATCH
     * SET element.PropertyName = 'Random Value'
     * SET element.OtherProperty = 32
     * RETURN element
     */
    public Node updateNode(String propertyName, String propertyValue, StructuredRecord input) {
        StringBuilder queryBuilder =
                new StringBuilder("MATCH (m {" + propertyName + ": '" + propertyValue + "'}) ON MATCH");
        for (Schema.Field field : input.getSchema().getFields()) {
            if (field.getSchema().getType().isSimpleType()) {
                queryBuilder.append(" SET m.").append(field.getName());
                if (NUMERIC_TYPES.contains(field.getSchema().getType())) {
                    queryBuilder.append("=").append(input.get(field.getName()).toString());
                } else {
                    queryBuilder.append("='").append(input.get(field.getName()).toString()).append("'");
                }
            }
        }
        queryBuilder.append(" RETURN m");
        String query = queryBuilder.toString();
        LOG.info(query);
        return session.writeTransaction(tx -> {
            Result result = tx.run(query);
            return result.single().get(0).asNode();
        });
    }

    /**
     * MATCH (parent {id: '1abce5e1-b7fb-11ea-82ff-97299e06153c'})
     * MERGE (parent)<-[:BELONGS]-(m:ElementName {Id: '3de87cb7-b7fb-11ea-82ff-97299e06153c',
     *          PropertyName: 'Random Value', OtherProperty: '32'})
     * RETURN m
     */
    public Node createNode(StructuredRecord input, String idPropertyName) {
        Objects.requireNonNull(input.getSchema());
        Objects.requireNonNull(input.getSchema().getFields());
        StringBuilder queryBuilder = new StringBuilder("MERGE (m {");
        for (Schema.Field field : input.getSchema().getFields()) {
            if (Schema.Type.RECORD.equals(field.getSchema().getType())) {
                // TODO: skipped for the time being
            } else if (field.getSchema().getType().isSimpleType()) {
                queryBuilder.append(field.getName()).append(":");
                if (NUMERIC_TYPES.contains(field.getSchema().getType())) {
                    queryBuilder.append(input.get(field.getName()).toString());
                } else {
                    queryBuilder.append("'").append(input.get(field.getName()).toString()).append("'");
                }
            }
        }
        queryBuilder.append("}) RETURN m");
        String query = queryBuilder.toString();
        LOG.info(query);
        return session.writeTransaction(tx -> {
            Result result = tx.run(query);
            return result.single().get(0).asNode();
        });
    }
}
