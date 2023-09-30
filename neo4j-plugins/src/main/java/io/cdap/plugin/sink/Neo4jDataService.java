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
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.types.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Neo4j specific data service
 */
public class Neo4jDataService {
    private static final Logger LOG = LoggerFactory.getLogger(Neo4jDataService.class);
    private static final String COLON = ":";
    private static final String EQUAL = "=";

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
     * @return Node by unique identifier or null if no Node found or if more than one Node was returned
     */
    public Record getUniqueNodeByProperty(String propertyName, String propertyValue) {
        String query = "MATCH (n {" + propertyName + ": '" + propertyValue + "'}) RETURN n";
        LOG.info(query);
        Result result = session.run(query);
        if (result.hasNext()) {
            List<Record> results = result.list();
            if (results.size() > 1) {
                LOG.warn("Returned more than one result");
                return null;
            }
            return results.get(0);
        }
        return null;
    }

    /**
     * MATCH (element {id: '3de87cb7-b7fb-11ea-82ff-97299e06153c'})
     * SET element.PropertyName = 'Random Value'
     * SET element.OtherProperty = 32
     * RETURN element
     */
    public Node updateNode(String propertyName, String propertyValue, StructuredRecord input) {
        LOG.info("Node update");
        Objects.requireNonNull(input.getSchema());
        Objects.requireNonNull(input.getSchema().getFields());
        StringBuilder queryBuilder =
                new StringBuilder("MATCH (m {" + propertyName + ": '" + propertyValue + "'})");
        for (Schema.Field field : input.getSchema().getFields()) {
            if (field.getSchema().getType().isSimpleType()) {
                queryBuilder.append(" SET m.");
                queryBuilder.append(
                        processPropertyIntoQuery(input, field, field.getSchema().getType(), EQUAL, true));
            } else if (Schema.Type.UNION.equals(field.getSchema().getType())) {
                queryBuilder.append(" SET m.");
                queryBuilder.append(
                        processPropertyIntoQuery(input, field, field.getSchema().getNonNullable().getType(),
                                EQUAL, true));
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
     * PropertyName: 'Random Value', OtherProperty: '32'})
     * RETURN m
     */
    public Node createNode(StructuredRecord input) {
        LOG.info("Create new node from: {}", input.toString());
        Objects.requireNonNull(input.getSchema());
        Objects.requireNonNull(input.getSchema().getFields());
        StringBuilder queryBuilder = new StringBuilder("MERGE (m {");
        List<String> ownProperties = new ArrayList<>();
        for (Schema.Field field : input.getSchema().getFields()) {
            LOG.info("Field: {}", field.toString());
            LOG.info("Field type {}", field.getSchema().getType());
            if (Schema.Type.RECORD.equals(field.getSchema().getType())) {
                LOG.info("It's record");
                // TODO: skipped for the time being
            } else if (field.getSchema().getType().isSimpleType()) {
                LOG.info("It's simple type");
                ownProperties.add(
                        processPropertyIntoQuery(input, field, field.getSchema().getType(), COLON, false));
            } else if (Schema.Type.UNION.equals(field.getSchema().getType())) {
                ownProperties.add(
                        processPropertyIntoQuery(input, field, field.getSchema().getNonNullable().getType(),
                                COLON, false));
            } else {
                LOG.warn("Unrecognized field type");
            }
        }
        queryBuilder.append(String.join(",", ownProperties));
        queryBuilder.append("}) RETURN m");
        String query = queryBuilder.toString();
        LOG.info(query);
        return session.writeTransaction(tx -> {
            Result result = tx.run(query);
            return result.single().get(0).asNode();
        });
    }

    // TODO: null values have to be processed additionally as in this setup records are created differently
    private String processPropertyIntoQuery(StructuredRecord input, Schema.Field field, Schema.Type type,
                                            String delimiter, Boolean addNull) {
        String subQuery = field.getName() + delimiter;
        Object dataField = input.get(field.getName());
        if (addNull && (dataField == null || dataField.toString().isEmpty())) {
            return subQuery + "null";
        } else {
            if (Schema.Type.STRING.equals(type)) {
                return subQuery + "'" + dataField + "'";
            } else {
                return subQuery + dataField;
            }
        }
    }
}
