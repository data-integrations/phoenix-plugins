/*
 * Copyright © 2018 Cask Data, Inc.
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

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.cdap.format.RecordPutTransformer;
import co.cask.hydrator.common.Constants;
import co.cask.hydrator.common.ReferenceBatchSink;
import co.cask.hydrator.common.SchemaValidator;
import co.cask.hydrator.common.batch.JobUtils;
import co.cask.hydrator.common.batch.sink.SinkOutputFormatProvider;
import co.cask.hydrator.plugin.ConnectionConfig;
import co.cask.hydrator.plugin.DBManager;
import co.cask.hydrator.plugin.DBRecord;
import co.cask.hydrator.plugin.FieldCase;
import co.cask.hydrator.plugin.common.Properties;
import co.cask.hydrator.plugin.common.TableSinkConfig;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.phoenix.mapreduce.PhoenixOutputFormat;
import org.apache.phoenix.mapreduce.util.PhoenixMapReduceUtil;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import javax.annotation.Nullable;

/**
 * CDAP Table Dataset Batch Sink.
 */
@Plugin(type = "batchsink")
@Name("PhoenixSink")
@Description("Writes records to a Table with one record field mapping to the Table rowkey," +
  " and all other record fields mapping to Table columns.")
public class PhoenixSink extends BatchSink<StructuredRecord, byte[], Put> {

  private final Config config;
  private final Schema outputSchema;

  private final DBManager dbManager;
  private Class<? extends Driver> driverClass;
  private int [] columnTypes;
  private List<String> columns;

  public PhoenixSink(Config config) {
    this.config = config;
    this.outputSchema = createOutputSchema();
  }

  private String getJDBCPluginId() {
    return String.format("%s.%s.%s", "sink", dbSinkConfig.jdbcPluginType, dbSinkConfig.jdbcPluginName);
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);

    // NOTE: this is done only for testing, once CDAP-4575 is implemented, we can use this schema in initialize
    pipelineConfigurer.getStageConfigurer().setOutputSchema(outputSchema);
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    driverClass = context.loadPluginClass(getJDBCPluginId());
    setResultSetMetadata();
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<byte[], DBWritable>> emitter) throws Exception {
    Put put = recordPutTransformer.toPut(input);
    emitter.emit(new KeyValue<>(put.getRow(), put));
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    Job job = JobUtils.createInstance();
    PhoenixMapReduceUtil.setOutput(job, config.tableName, config.columns);
    context.addOutput(Output.of(config.referenceName,
                                new SinkOutputFormatProvider(PhoenixOutputFormat.class, job.getConfiguration())));
  }

  private void setResultSetMetadata() throws Exception {
    Map<String, Integer> columnToType = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    dbManager.ensureJDBCDriverIsAvailable(driverClass);

    Connection connection;
    if (config.user == null) {
      connection = DriverManager.getConnection(config.connectionString);
    } else {
      connection = DriverManager.getConnection(config.connectionString, config.user, config.password);
    }

    try {
      try (Statement statement = connection.createStatement();
           // Run a query against the DB table that returns 0 records, but returns valid ResultSetMetadata
           // that can be used to construct DBRecord objects to sink to the database table.
           ResultSet rs = statement.executeQuery(String.format("SELECT %s FROM %s WHERE 1 = 0",
                                                               config.columns, config.tableName))
      ) {
        ResultSetMetaData resultSetMetadata = rs.getMetaData();
//        FieldCase fieldCase = FieldCase.toFieldCase(config.columnNameCase);
        FieldCase fieldCase = FieldCase.LOWER;
        // JDBC driver column indices start with 1
        for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
          String name = resultSetMetadata.getColumnName(i + 1);
          int type = resultSetMetadata.getColumnType(i + 1);
          if (fieldCase == FieldCase.LOWER) {
            name = name.toLowerCase();
          } else if (fieldCase == FieldCase.UPPER) {
            name = name.toUpperCase();
          }
          columnToType.put(name, type);
        }
      }
    } finally {
      connection.close();
    }

    columns = ImmutableList.copyOf(Splitter.on(",").omitEmptyStrings().trimResults().split(config.columns));
    columnTypes = new int[columns.size()];
    for (int i = 0; i < columnTypes.length; i++) {
      String name = columns.get(i);
      Preconditions.checkArgument(columnToType.containsKey(name), "Missing column '%s' in SQL table", name);
      columnTypes[i] = columnToType.get(name);
    }
  }

  /**
   * Configurations for the {@link PhoenixSink} plugin.
   */
  public static final class Config extends ConnectionConfig {

    @Name(Constants.Reference.REFERENCE_NAME)
    @Description(Constants.Reference.REFERENCE_NAME_DESCRIPTION)
    public String referenceName;

    @Description(
      "Table to write to."
    )
    @Macro
    private String tableName;

    @Description(
      "Comma-separated list of columns."
    )
    @Macro
    private String columns;
  }
}
