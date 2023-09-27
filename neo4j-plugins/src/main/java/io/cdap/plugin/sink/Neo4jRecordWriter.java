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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Neo4j CDAP Sink record writer
 */
public class Neo4jRecordWriter extends RecordWriter<StructuredRecord, NullWritable> {
    private static final Logger LOG = LoggerFactory.getLogger(Neo4jRecordWriter.class);

    private final Session session;

    public Neo4jRecordWriter(Session session) {
        this.session = session;
    }

    @Override
    public void write(StructuredRecord structuredRecord, NullWritable nullWritable) {
        LOG.info("Write into Neo4j");
        List<Schema.Field> fields = structuredRecord.getSchema().getFields();
        StringBuilder querySB = new StringBuilder("CREATE (a:TODO) SET ");
        for (Schema.Field field : fields) {
            if (field.getSchema().isSimpleOrNullableSimple()) {
                Object value = structuredRecord.get(field.getName());
                querySB.append("a.").append(field.getName()).append(" = ").append(value);
            }
        }
        querySB.append(" RETURN id(a)");
        session.writeTransaction(tx -> {
            Result result = tx.run(querySB.toString());
            Long nodeId = result.single().get(0).asLong();
            LOG.info("Created node with id: {}", nodeId);
            return nodeId;
        });
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) {
        LOG.info("Close writer");
        if (session != null) {
            session.close();
        }
    }
}
