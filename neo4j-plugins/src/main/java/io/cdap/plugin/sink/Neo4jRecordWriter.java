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
import org.neo4j.driver.types.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Neo4j CDAP Sink record writer
 */
public class Neo4jRecordWriter extends RecordWriter<StructuredRecord, NullWritable> {
    private static final Logger LOG = LoggerFactory.getLogger(Neo4jRecordWriter.class);
    private static final String ID = "UID";

    private final Session session;
    private final Neo4jDataService dataService;

    public Neo4jRecordWriter(Session session) {
        this.session = session;
        dataService = new Neo4jDataService(session);
    }

    @Override
    public void write(StructuredRecord structuredRecord, NullWritable nullWritable) {
        String uid = structuredRecord.get(ID);
        Node existedNode = dataService.getUniqueNodeByProperty(ID, uid);
        if (existedNode != null) {
            Node updatedNode = dataService.updateNode(ID, uid, structuredRecord);
            if (updatedNode == null) {
                LOG.error("Node with id {} was not updated", uid);
            }
        } else {
            Node newNode = dataService.createNode(structuredRecord);
            if (newNode == null) {
                LOG.error("Node creation process has failed");
            }
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) {
        LOG.info("Close writer");
        if (session != null) {
            session.close();
        }
    }
}
