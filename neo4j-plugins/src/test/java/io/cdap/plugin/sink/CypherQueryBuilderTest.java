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

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class CypherQueryBuilderTest {
    private static final Schema INNTER_SCHEMA = Schema.recordOf(
            "Holding",
            Schema.Field.of("Metadata", Schema.unionOf(Schema.of(Schema.Type.NULL),
                    Schema.of(Schema.Type.STRING))),
            Schema.Field.of("UID", Schema.unionOf(Schema.of(Schema.Type.NULL),
                    Schema.of(Schema.Type.STRING))),
            Schema.Field.of("Name", Schema.unionOf(Schema.of(Schema.Type.NULL),
                    Schema.of(Schema.Type.STRING))));
    private static final Schema BODY_SCHEMA = Schema.recordOf(
            "Dictionary",
            Schema.Field.of("Metadata", Schema.unionOf(Schema.of(Schema.Type.NULL),
                    Schema.of(Schema.Type.STRING))),
            Schema.Field.of("UID", Schema.unionOf(Schema.of(Schema.Type.NULL),
                    Schema.of(Schema.Type.STRING))),
            Schema.Field.of("Name", Schema.unionOf(Schema.of(Schema.Type.NULL),
                    Schema.of(Schema.Type.STRING))),
            Schema.Field.of("ListData", Schema.arrayOf(
                    Schema.unionOf(Schema.of(Schema.Type.NULL), INNTER_SCHEMA))),
            Schema.Field.of("IsSomething", Schema.unionOf(Schema.of(Schema.Type.NULL),
                    Schema.of(Schema.Type.BOOLEAN))));

    @Test @Ignore
    public void generateMatchStatementsTest() throws IOException {
        StructuredRecord inner = StructuredRecord.builder(Schema.parseJson(INNTER_SCHEMA.toString()))
                .set("Metadata", "Holding")
                .set("UID", "345jkj-mn3n45-34m5n34")
                .set("Name", "Input")
                .build();
        StructuredRecord input = StructuredRecord.builder(Schema.parseJson(BODY_SCHEMA.toString()))
                .set("Metadata", "Alliance")
                .set("UID", "345jkj-mn3n45-34m5n34")
                .set("Name", "Input")
                .set("ListData", Collections.singletonList(inner))
                .set("IsSomething", false)
                .build();
        List<String> strings = CypherQueryBuilder.generateMatchStatements(input);
        Assert.assertEquals(1, strings.size());
    }
}
