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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * {
 *   "ElementName": {
 *     "Id": "3de87cb7-b7fb-11ea-82ff-97299e06153c",
 *     "Metadata": "ElementName",
 *     "PropertyName": "Random Value",
 *     "OtherProperty": 32,
 *     "ParentObject": {
 *       "Id": "1abce5e1-b7fb-11ea-82ff-97299e06153c",
 *       "Metadata": "ParentObject"
 *       "ParentProperty": "Other Value"
 *     },
 *     "StaticData": "Something",
 *     "RelaredObject": {
 *       "Id": "j453j45h3-kj34-kj345-kj34-k3j45kj3k",
 *       "Metadata": "RelaredObject",
 *       "Data": "Just Data",
 *     }
 *   }
 * }
 * (RelatedObject)-[:USES]->(ElementName)-[:BELONGS]->(ParentObject)
 */
public class CypherQueryBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(CypherQueryBuilder.class);
    private static final String ID = "Id";
    private static final String TYPE = "Metadata";
    private static final Pattern NODES_PATTERN = Pattern.compile("\\((\\w+)\\)");
    private static final Pattern RELATION_PATTERN = Pattern.compile("\\)([-<>]\\[:\\w+\\][-><]+)\\(");
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

    public static void getInnerObjectAsMatch(StructuredRecord input) {

    }

}
