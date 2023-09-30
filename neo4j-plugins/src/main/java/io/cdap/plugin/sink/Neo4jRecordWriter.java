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
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.types.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Neo4j CDAP Sink record writer
 */
public class Neo4jRecordWriter extends RecordWriter<StructuredRecord, StructuredRecord> {
    private static final Logger LOG = LoggerFactory.getLogger(Neo4jRecordWriter.class);
    private static final String ID = "UID";

    private final Session session;
    private final Neo4jDataService dataService;

    public Neo4jRecordWriter(Session session) {
        this.session = session;
        dataService = new Neo4jDataService(session);
    }

    @Override
    public void write(StructuredRecord key, StructuredRecord value) {
        String uid = value.get(ID);
        LOG.info("Start processing element with id: {}", uid);
        LOG.info("Just for fun: {}", (String) key.get(ID));
        Record existedNode = dataService.getUniqueNodeByProperty(ID, uid);
        if (existedNode != null) {
            Node updatedNode = dataService.updateNode(ID, uid, value);
            if (updatedNode == null) {
                LOG.error("Node with id {} was not updated", uid);
            }
        } else {
            Node newNode = dataService.createNode(value);
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
