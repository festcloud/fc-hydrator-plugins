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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Metadata;
import io.cdap.cdap.api.annotation.MetadataProperty;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.data.schema.Schema.Field;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.plugin.common.ReferenceBatchSink;
import io.cdap.plugin.common.ReferencePluginConfig;
import io.cdap.plugin.connector.Neo4jConnector;
import io.cdap.plugin.sink.objects.RelationObj;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static io.cdap.plugin.common.Neo4jConstants.DATABASE;
import static io.cdap.plugin.common.Neo4jConstants.PASSWORD;
import static io.cdap.plugin.common.Neo4jConstants.URL;
import static io.cdap.plugin.common.Neo4jConstants.USER;

/**
 * Neo4j CDAP Sink
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("Neo4j")
@Description("Neo4j Sink")
@Metadata(properties = {@MetadataProperty(key = Connector.PLUGIN_TYPE, value = Neo4jConnector.NAME)})
public class Neo4jSink extends ReferenceBatchSink<StructuredRecord, StructuredRecord, StructuredRecord> {

    private static final Logger LOG = LoggerFactory.getLogger(Neo4jSink.class);

    private final Neo4jSinkConfig config;
    private List<RelationObj> relations;

    public Neo4jSink(Neo4jSinkConfig config) {
        super(new ReferencePluginConfig(config.getReferenceName()));
        this.config = config;
    }

    @Override
    public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
        LOG.info("Call configurePipeline function");
        super.configurePipeline(pipelineConfigurer);
        FailureCollector collector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
        config.validate(collector);
        Schema inputSchema = Objects.requireNonNull(pipelineConfigurer.getStageConfigurer().getInputSchema());
        relations = config.getRelations(getRecordNames(inputSchema));
        super.configurePipeline(pipelineConfigurer);

    }

    @Override
    public void prepareRun(BatchSinkContext context) throws Exception {
        LOG.info("Call prepareRun function");
        FailureCollector collector = context.getFailureCollector();
        config.validate(collector);
        Schema inputSchema = Objects.requireNonNull(context.getInputSchema());
        relations = config.getRelations(getRecordNames(inputSchema));
        collector.getOrThrowException();
        context.addOutput(Output.of(config.getReferenceName(), new Neo4jOutputFormatProvider(config)));
    }

    @Override
    public void onRunFinish(boolean succeeded, BatchSinkContext context) {
        LOG.info("Call onRunFinish function");
        super.onRunFinish(succeeded, context);
        // write metrics
    }

    private List<String> getRecordNames(Schema inputSchema) {
        List<String> recordNames = new ArrayList<>();
        List<Field> fields = inputSchema.getFields();
        for (Field field : fields) {
            if (Schema.Type.RECORD == field.getSchema().getType()) {
                recordNames.add(field.getName());
            }
        }
        return recordNames;
    }

    private static class Neo4jOutputFormatProvider implements OutputFormatProvider {
        private final Neo4jSinkConfig config;

        Neo4jOutputFormatProvider(Neo4jSinkConfig config) {
            this.config = config;
        }

        @Override
        public String getOutputFormatClassName() {
            return Neo4jOutputFormat.class.getName();
        }

        @Override
        public Map<String, String> getOutputFormatConfiguration() {
            Map<String, String> configMap = new HashMap<>();
            configMap.put(URL, config.getUrl());
            configMap.put(USER, config.getUser());
            configMap.put(PASSWORD, config.getPassword());
            configMap.put(DATABASE, config.getDatabase());
            return configMap;
        }
    }
}
