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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;

import java.io.IOException;

import static io.cdap.plugin.common.Neo4jConstants.PASSWORD;
import static io.cdap.plugin.common.Neo4jConstants.URL;
import static io.cdap.plugin.common.Neo4jConstants.USER;

/**
 * Neo4j CDAP Sink output format
 */
public class Neo4jOutputFormat extends OutputFormat<StructuredRecord, NullWritable> {

    private Driver driver;

    @Override
    public RecordWriter<StructuredRecord, NullWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) {
        Configuration configuration = taskAttemptContext.getConfiguration();
        return new Neo4jRecordWriter(getSession(configuration));
    }

    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {

    }

    private Session getSession(Configuration configuration) {
        if (driver == null) {
            driver = GraphDatabase.driver(configuration.get(URL),
                    AuthTokens.basic(configuration.get(USER), configuration.get(PASSWORD)));
        }
        return driver.session();
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) {
        return new OutputCommitter() {
            @Override
            public void setupJob(JobContext jobContext) throws IOException {

            }

            @Override
            public void setupTask(TaskAttemptContext taskAttemptContext) throws IOException {

            }

            @Override
            public boolean needsTaskCommit(TaskAttemptContext taskAttemptContext) throws IOException {
                return false;
            }

            @Override
            public void commitTask(TaskAttemptContext taskAttemptContext) throws IOException {

            }

            @Override
            public void abortTask(TaskAttemptContext taskAttemptContext) throws IOException {

            }
        };
    }
}
