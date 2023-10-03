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
import org.apache.commons.math3.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * {
 * "ElementName": {
 * "Id": "3de87cb7-b7fb-11ea-82ff-97299e06153c",
 * "Metadata": "ElementName",
 * "PropertyName": "Random Value",
 * "OtherProperty": 32,
 * "ParentObject": {
 * "Id": "1abce5e1-b7fb-11ea-82ff-97299e06153c",
 * "Metadata": "ParentObject"
 * "ParentProperty": "Other Value"
 * },
 * "StaticData": "Something",
 * "RelaredObject": {
 * "Id": "j453j45h3-kj34-kj345-kj34-k3j45kj3k",
 * "Metadata": "RelaredObject",
 * "Data": "Just Data",
 * }
 * }
 * }
 * (RelatedObject)-[:USES]->(ElementName)-[:BELONGS]->(ParentObject)
 */
public class CypherQueryBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(CypherQueryBuilder.class);
    private static final String COLON = ":";
    private static final Pattern NODES_PATTERN = Pattern.compile("\\((\\w+)\\)");
    private static final Pattern RELATION_PATTERN = Pattern.compile("\\)([-<>]\\[:\\w+\\][-><]+)\\(");
    public static final String[] FIELD_METADATA = new String[] {"Metadata", "ОбєктМетаданих"};
    public static final String[] FIELD_UID = new String[] {"UID", "УІД"};


    public static void parseCypherMapping(String cypherMapping) {
        Matcher nodesMatcher = NODES_PATTERN.matcher(cypherMapping);
        List<String> nodes = new ArrayList<>();
        while (nodesMatcher.find()) {
            nodes.add(nodesMatcher.group(1));
        }

        Matcher relationMatcher = RELATION_PATTERN.matcher(cypherMapping);
        List<String> relations = new ArrayList<>();
        while (relationMatcher.find()) {
            relations.add(relationMatcher.group(1));
        }

        System.out.println(nodes);
        System.out.println(relations);
    }

    public static Map<String, String> generateMatchStatements(StructuredRecord input) {
        Objects.requireNonNull(input.getSchema());
        Objects.requireNonNull(input.getSchema().getFields());

        Map<String, String> matchStatements = new HashMap<>();
        for (Schema.Field field : input.getSchema().getFields()) {
            if (Schema.Type.RECORD.equals(field.getSchema().getType())) {
                LOG.info("Process RECORD");
                Object fieldValue = input.get(field.getName());
                if (fieldValue instanceof Collection) {
                    matchStatements.putAll(generateMatchStatementFromArray(input, field));
                } else if (fieldValue instanceof StructuredRecord) {
                    String metadataLabel = getMetadataLabel(input);
                    String matchStatement = recordToMatchStatement((StructuredRecord) fieldValue, metadataLabel);
                    if (metadataLabel != null && matchStatement != null) {
                        matchStatements.put(metadataLabel, matchStatement);
                    }
                }
            } else if (Schema.Type.ARRAY.equals(field.getSchema().getType())) {
                LOG.info("Process ARRAY");
                matchStatements.putAll(generateMatchStatementFromArray(input, field));
            }
        }
        return matchStatements;
    }

    private static Map<String, String> generateMatchStatementFromArray(StructuredRecord input, Schema.Field field) {
        Map<String, String> matchStatements = new HashMap<>();
        for (Object inner : ((Collection) Objects.requireNonNull(input.get(field.getName())))) {
            if (inner instanceof StructuredRecord) {
                String metadataLabel = getMetadataLabel((StructuredRecord) inner);
                String matchStatement = recordToMatchStatement((StructuredRecord) inner, metadataLabel);
                if (metadataLabel != null && matchStatement != null) {
                    matchStatements.put(metadataLabel,
                            recordToMatchStatement((StructuredRecord) inner, metadataLabel));
                }
            }
        }
        return matchStatements;
    }

    public static String generateMergeQuery(StructuredRecord input) {
        Objects.requireNonNull(input.getSchema());
        Objects.requireNonNull(input.getSchema().getFields());
        Object metadata = getMetadataLabel(input);
        StringBuilder queryBuilder = new StringBuilder("MERGE (m:" + metadata + " {");
        List<String> ownProperties = new ArrayList<>();
        for (Schema.Field field : input.getSchema().getFields()) {
            LOG.info("Field: {}", field.toString());
            if (field.getSchema().getType().isSimpleType()) {
                ownProperties.add(
                        processPropertyIntoQuery(input, field, field.getSchema().getType(), COLON, false));
            } else if (Schema.Type.UNION.equals(field.getSchema().getType())) {
                ownProperties.add(
                        processPropertyIntoQuery(input, field, field.getSchema().getNonNullable().getType(),
                                COLON, false));
            }
        }
        queryBuilder.append(String.join(",", ownProperties));
        queryBuilder.append("})");
        return queryBuilder.toString();
    }

    // TODO: null values have to be processed additionally as in this setup records are created differently
    static String processPropertyIntoQuery(StructuredRecord input, Schema.Field field, Schema.Type type,
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

    private static String recordToMatchStatement(StructuredRecord record, String label) {
        String matchStatementTemplate = "MATCH (%s {%s: '%s'})";
        Pair<String, String> idFieldValue = getRecordId(record);
        if (idFieldValue != null) {
            return String.format(matchStatementTemplate, label, idFieldValue.getKey(), idFieldValue.getValue());
        } else {
            LOG.error("Provided id property name is not present in the inner element");
            // TODO: possibly throw an error
            return null;
        }
    }

    private static Pair<String, String> getRecordId(StructuredRecord record) {
        for (String idField : FIELD_UID) {
            Object idValue = record.get(idField);
            if (idValue != null) {
                return Pair.create(idField, idValue.toString());
            }
        }
        return null;
    }

    private static String getMetadataLabel(StructuredRecord input) {
        for (String metadataField : FIELD_METADATA) {

            Object metadataValue = input.get(metadataField);
            if (metadataValue != null) {
                return metadataValue.toString().toLowerCase();
            }
        }
        // TODO: possibly throw an error
        return null;
    }
}
