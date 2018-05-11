/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package co.cask;

import co.cask.cdap.api.Config;
import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.cdap.format.RecordPutTransformer;
import co.cask.hydrator.common.SchemaValidator;
import co.cask.hydrator.common.SourceInputFormatProvider;
import co.cask.hydrator.common.batch.JobUtils;
import co.cask.hydrator.plugin.DBRecord;
import co.cask.hydrator.plugin.common.Properties;
import co.cask.hydrator.plugin.common.TableSinkConfig;
import co.cask.hydrator.plugin.batch.sink.BatchWritableSink;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.hadoop.mapreduce.Job;

import org.apache.phoenix.mapreduce.util.PhoenixMapReduceUtil;
import org.apache.phoenix.mapreduce.PhoenixInputFormat;
import org.apache.phoenix.mapreduce.PhoenixOutputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.io.Writable;



import java.sql.PreparedStatement;


import java.util.HashMap;
import java.util.Map;

/**
 * CDAP Table Dataset Batch Sink.
 */
@Plugin(type = "batchsink")
@Name("PhoenixSink")
@Description("Writes records to a Table with one record field mapping to the Table rowkey," +
  " and all other record fields mapping to Table columns.")
public class PhoenixSink extends BatchWritableSink<StructuredRecord, byte[], Put> {
//  public class PhoenixSink extends BatchWritableSink<StructuredRecord, byte[], DBWritable> {
  private final TableSinkConfig tableSinkConfig;
  private RecordPutTransformer recordPutTransformer;
//  private final Config config;

  public PhoenixSink(TableSinkConfig tableSinkConfig) {
    super(tableSinkConfig);
    this.tableSinkConfig = tableSinkConfig;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    Preconditions.checkArgument(tableSinkConfig.containsMacro(Properties.Table.PROPERTY_SCHEMA_ROW_FIELD) ||
                                  !Strings.isNullOrEmpty(tableSinkConfig.getRowField()),
                                "Row field must be given as a property.");
    Schema outputSchema =
      SchemaValidator.validateOutputSchemaAndInputSchemaIfPresent(tableSinkConfig.getSchemaStr(),
                                                                  tableSinkConfig.getRowField(), pipelineConfigurer);
    if ((outputSchema != null) && (outputSchema.getFields().size() == 1)) {
      String fieldName = outputSchema.getFields().get(0).getName();
      if (fieldName.equals(tableSinkConfig.getRowField())) {
        throw new IllegalArgumentException(
          String.format("Output schema should have columns other than rowkey."));
      }
    }
    // NOTE: this is done only for testing, once CDAP-4575 is implemented, we can use this schema in initialize
    pipelineConfigurer.getStageConfigurer().setOutputSchema(outputSchema);


//    PhoenixMapReduceUtil.setOutput(job, DBRecord.class, config.tableName, config.tableColumns);

  }

  @Override
  protected boolean shouldSkipCreateAtConfigure() {
    return tableSinkConfig.containsMacro(Properties.Table.PROPERTY_SCHEMA) ||
      tableSinkConfig.containsMacro(Properties.Table.PROPERTY_SCHEMA_ROW_FIELD);
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    Schema outputSchema = null;
    // If a schema string is present in the properties, use that to construct the outputSchema and pass it to the
    // recordPutTransformer
    String schemaString = tableSinkConfig.getSchemaStr();
    if (schemaString != null) {
      outputSchema = Schema.parseJson(schemaString);
    }
    recordPutTransformer = new RecordPutTransformer(tableSinkConfig.getRowField(), outputSchema);
  }

  @Override
  protected Map<String, String> getProperties() {
    Map<String, String> properties;
    properties = new HashMap<>(tableSinkConfig.getProperties().getProperties());

    properties.put(Properties.BatchReadableWritable.NAME, tableSinkConfig.getName());
    properties.put(Properties.BatchReadableWritable.TYPE, Table.class.getName());
    return properties;
  }

//  @Override
//  public void transform(StructuredRecord input, Emitter<KeyValue<byte[], Put>> emitter) throws Exception {
//    Put put = recordPutTransformer.toPut(input);
//    emitter.emit(new KeyValue<>(put.getRow(), put));
//  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<byte[], DBWritable>> emitter) throws Exception {
    Put put = recordPutTransformer.toPut(input);
    emitter.emit(new KeyValue<>(put.getRow(), put));
    PhoenixMapReduceUtil.setOutput(job, "TABLE_NAME", "<TABLE_COLUMNS>");
  }

//  @Override
//  public void prepareRun(BatchSinkContext context) throws Exception {
//    Job job = JobUtils.createInstance();
//
////    HiveSinkOutputFormatProvider sinkOutputFormatProvider = new HiveSinkOutputFormatProvider(job, config);
////    HCatSchema hiveSchema = sinkOutputFormatProvider.getHiveSchema();
//
//    context.getArguments().set(config.getDBTable(), GSON.toJson(hiveSchema));
//    context.addOutput(Output.of(config.referenceName, sinkOutputFormatProvider));
//  }

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    Job job = createJob();
    PhoenixMapReduceUtil.setOutput(job, DBRecord.class, config.tableName, config.inputQuery);
    context.setInput(Input.of(config.referenceName,
            new SourceInputFormatProvider(PhoenixInputFormat.class, job.getConfiguration())));
  }

}
