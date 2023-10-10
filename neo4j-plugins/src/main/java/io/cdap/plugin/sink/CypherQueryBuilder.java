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
import io.cdap.plugin.sink.objects.RelationDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

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
    private static final String LEFT_RELATION = "<";
    private static final String RIGHT_RELATION = ">";
    public static final String FIELD_METADATA = "metadata";
    public static final String FIELD_UID = "uid";

    // MERGE (a)<-[:BELONGS]-(m)
    public static String generateMergeRelations(List<RelationDto> relationDtoList) {
        return relationDtoList.stream().map(relationDto -> {
            if (LEFT_RELATION.equals(relationDto.getDirection())) {
                return String.join("",
                        "MERGE (", relationDto.getElementName(), ")-[:", relationDto.getRelationName(), "]->(m)");
            } else if (RIGHT_RELATION.equals(relationDto.getDirection())) {
                return  String.join("",
                        "MERGE (", relationDto.getElementName(), ")<-[:", relationDto.getRelationName(), "]-(m)");
            } else {
                return  String.join("",
                        "MERGE (", relationDto.getElementName(), ")-[:", relationDto.getRelationName(), "]-(m)");
            }
        }).collect(Collectors.joining(" "));
    }

    public static List<String> generateMatchStatements(StructuredRecord input) {
        Objects.requireNonNull(input.getSchema());
        Objects.requireNonNull(input.getSchema().getFields());

        List<String> matchStatements = new ArrayList<>();
        for (Schema.Field field : input.getSchema().getFields()) {
            if (Schema.Type.RECORD.equals(field.getSchema().getType())) {
                LOG.info("Process RECORD");
                String fieldName = field.getName();
                Object fieldValue = input.get(fieldName);
                if (fieldValue instanceof Collection) {
                    matchStatements.addAll(generateMatchStatementFromArray(input, field));
                } else if (fieldValue instanceof StructuredRecord) {
                    StructuredRecord structuredFieldValue = (StructuredRecord) fieldValue;
                    String matchStatement = recordToMatchStatement(structuredFieldValue, fieldName);
                    matchStatements.add(matchStatement);
                }
            } else if (Schema.Type.ARRAY.equals(field.getSchema().getType())) {
                LOG.info("Process ARRAY");
                matchStatements.addAll(generateMatchStatementFromArray(input, field));
            } else if (Schema.Type.UNION.equals(field.getSchema().getType())) {
                if (Schema.Type.RECORD.equals(field.getSchema().getNonNullable().getType())) {
                    LOG.info("Process UNION");
                    String fieldName = field.getName();
                    Object fieldValue = input.get(fieldName);
                    if (fieldValue instanceof Collection) {
                        matchStatements.addAll(generateMatchStatementFromArray(input, field));
                    } else if (fieldValue instanceof StructuredRecord) {
                        StructuredRecord structuredFieldValue = (StructuredRecord) fieldValue;
                        String matchStatement = recordToMatchStatement(structuredFieldValue, fieldName);
                        matchStatements.add(matchStatement);
                    }
                }
            }
        }
        return matchStatements;
    }

    private static List<String> generateMatchStatementFromArray(StructuredRecord input, Schema.Field field) {
        List<String> matchStatements = new ArrayList<>();
        String fieldName = field.getName();
        for (Object inner : ((Collection) Objects.requireNonNull(input.get(fieldName)))) {
            if (inner instanceof StructuredRecord) {
                String matchStatement = recordToMatchStatement((StructuredRecord) inner, fieldName);
                matchStatements.add(matchStatement);
            }
        }
        return matchStatements;
    }

    public static String generateMergeQuery(StructuredRecord input) {
        Objects.requireNonNull(input.getSchema());
        Objects.requireNonNull(input.getSchema().getFields());
        Object metadata = input.get(FIELD_METADATA);
        StringBuilder queryBuilder = new StringBuilder("MERGE (m:" + metadata + " {");
        List<String> ownProperties = new ArrayList<>();
        for (Schema.Field field : input.getSchema().getFields()) {
            LOG.info("Field: {}", field.toString());
            if (field.getSchema().getType().isSimpleType()) {
                ownProperties.add(
                        processPropertyIntoQuery(input, field, field.getSchema().getType(), COLON, false));
            } else if (Schema.Type.UNION.equals(field.getSchema().getType())) {
                if (field.getSchema().getNonNullable().getType().isSimpleType()) {
                    ownProperties.add(
                            processPropertyIntoQuery(input, field, field.getSchema().getNonNullable().getType(),
                                    COLON, false));
                }
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
        String matchStatementTemplate = "MATCH (%s {uid: '%s'})";
        String idFieldValue = record.get(FIELD_UID);
        if (idFieldValue != null) {
            return String.format(matchStatementTemplate, label, idFieldValue);
        } else {
            LOG.error("Provided id property name is not present in the inner element");
            // TODO: possibly throw an error
            return null;
        }
    }
}
