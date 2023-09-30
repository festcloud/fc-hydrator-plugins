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
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;

import java.io.IOException;

/**
 * Data Service Test
 */
public class Neo4jDataServiceTest {

    private final Neo4jDataService dataService;

    public Neo4jDataServiceTest() {
        Session session = GraphDatabase.driver("bolt://localhost:7687",
                AuthTokens.basic("neo4j", "rootroot")).session();
        dataService = new Neo4jDataService(session);
    }

    private static final Schema BODY_SCHEMA = Schema.recordOf(
            "Dictionary",
            Schema.Field.of("Metadata", Schema.unionOf(Schema.of(Schema.Type.NULL),
                    Schema.of(Schema.Type.STRING))),
            Schema.Field.of("UID", Schema.unionOf(Schema.of(Schema.Type.NULL),
                    Schema.of(Schema.Type.STRING))),
            Schema.Field.of("Name", Schema.unionOf(Schema.of(Schema.Type.NULL),
                    Schema.of(Schema.Type.STRING))),
            Schema.Field.of("IsSomething", Schema.unionOf(Schema.of(Schema.Type.NULL),
                    Schema.of(Schema.Type.BOOLEAN))));
    @Test @Ignore
    public void createNode() throws IOException {
        StructuredRecord input = StructuredRecord.builder(Schema.parseJson(BODY_SCHEMA.toString()))
                .set("Metadata", "Holding")
                .set("UID", "345jkj-mn3n45-34m5n34")
                .set("Name", "")
                .set("IsSomething", false)
                .build();

        dataService.createNode(input);
    }

    @Test @Ignore
    public void testRead() {
        Record id = dataService.getUniqueNodeByProperty("id", "kj345k2j53k45");
        Assert.assertNotNull(id);
    }

    @Test @Ignore
    public void updateNode() throws IOException {
        StructuredRecord input = StructuredRecord.builder(Schema.parseJson(BODY_SCHEMA.toString()))
                .set("Metadata", "Holding")
                .set("UID", "345jkj-mn3n45-34m5n34")
                .set("Name", "")
                .set("IsSomething", false)
                .build();
        dataService.updateNode("UID", "345jkj-mn3n45-34m5n34", input);
    }
}
