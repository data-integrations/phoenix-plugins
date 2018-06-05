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
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.hydrator.common.Constants;
import co.cask.hydrator.common.batch.JobUtils;
import co.cask.hydrator.common.batch.sink.SinkOutputFormatProvider;
import co.cask.hydrator.plugin.ConnectionConfig;
import co.cask.hydrator.plugin.DBManager;
import co.cask.hydrator.plugin.DBRecord;
import co.cask.hydrator.plugin.DBUtils;
import co.cask.hydrator.plugin.DriverCleanup;
import co.cask.hydrator.plugin.FieldCase;
import co.cask.hydrator.plugin.JDBCDriverShim;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.phoenix.mapreduce.PhoenixOutputFormat;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.mapreduce.util.PhoenixMapReduceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.reflect.Reflection;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Apache Phoenix Batch Sink.
 */
//@Plugin(type = "batchsink")
@Name("PhoenixSink")
@Description("Writes records to an Apache Phoenix table.")
public class PhoenixSink extends BatchSink<StructuredRecord, DBRecord, NullWritable> {
  private static final Logger LOG = LoggerFactory.getLogger(PhoenixSink.class);

  private final Config config;

  private final DBManager dbManager;

  private Class<? extends Driver> driverClass;
  private int [] columnTypes;
  private List<String> columns;

  public PhoenixSink(Config config) {
    this.config = config;
    this.dbManager = new DBManager(config);
  }

  private String getJDBCPluginId() {
    return String.format("%s.%s.%s", "sink", config.jdbcPluginType, config.jdbcPluginName);
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    dbManager.validateJDBCPluginPipeline(pipelineConfigurer, getJDBCPluginId());
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    driverClass = context.loadPluginClass(getJDBCPluginId());
    setResultSetMetadata();
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    LOG.info("version 004 (setting classloader)");
    LOG.info("tableName = {}; pluginType = {}; pluginName = {}; connectionString = {}; columns = {}",
             config.tableName, config.jdbcPluginType, config.jdbcPluginName,
             config.connectionString, config.columns);

    // Load the plugin class to make sure it is available.
    Class<? extends Driver> driverClass = context.loadPluginClass(getJDBCPluginId());

    java.util.Properties info = new java.util.Properties();
    Driver driver = new JDBCDriverShim(driverClass.newInstance());
    DBUtils.deregisterAllDrivers(driverClass);
    DriverManager.registerDriver(driver);
    LOG.info("Driver is {}", driver);

    try {
      Connection conn =
        driver.connect("jdbc:phoenix:phx31815-1000.dev.continuuity.net:2181:/hbase-unsecure", info);
      LOG.info("Successfully get connection via driver {}", conn);
    } catch (Throwable t) {
      LOG.info("Failed to get connection via driver", t);
    }

    Driver driver2 = DriverManager.getDriver("jdbc:phoenix:phx31815-1000.dev.continuuity.net:2181:/hbase-unsecure;");
    LOG.info("Driver2 is {}", driver2);

//    LOG.info("Calling getConnection...");

//    try {
    try {
      Connection conn =
        driver2.connect("jdbc:phoenix:phx31815-1000.dev.continuuity.net:2181:/hbase-unsecure", info);
      LOG.info("Successfully called getConnection on driver2: {}", conn);
    } catch (Throwable t) {
      LOG.info("Failed to get connection via driver2", t);
    }
//
//
//      //    LOG.info("Called getConnection: {}",
//      //             driverClass.getMethod("connect", String.class, java.util.Properties.class)
//      //               .invoke(driverClass.newInstance(),
//      //                       "jdbc:phoenix:phx31815-1000.dev.continuuity.net:2181:/hbase-unsecure",
//      //                       info));
//    } catch (Throwable t) {
//      LOG.error("Failed: ", t);
//    }
//

    // Class.forName("org.apache.phoenix.jdbc.PhoenixDriver”);
    // make sure that the table exists
    ClassLoader oldCL = Thread.currentThread().getContextClassLoader();

    Thread.currentThread().setContextClassLoader(driverClass.getClassLoader());
    try {
//      ensureJDBCDriverIsAvailable(driverClass, config.connectionString, config.jdbcPluginType, config.jdbcPluginName);
      try {
        Preconditions.checkArgument(
          dbManager.tableExists(driverClass, config.tableName),
          "Table %s does not exist. Please check that the 'tableName' property " +
            "has been set correctly, and that the connection string %s points to a valid database.",
          config.tableName, config.connectionString);
      } finally {
        DBUtils.cleanup(driverClass);
      }
    } finally {
      Thread.currentThread().setContextClassLoader(oldCL);
    }

    Job job = JobUtils.createInstance();
    PhoenixMapReduceUtil.setOutput(job, config.tableName, config.columns);
    job.getConfiguration().set(PhoenixConfigurationUtil.MAPREDUCE_OUTPUT_CLUSTER_QUORUM,
                               "phx31815-1000.dev.continuuity.net");
    context.addOutput(Output.of(config.referenceName,
                                new SinkOutputFormatProvider(PhoenixOutputFormat.class, job.getConfiguration())));
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<DBRecord, NullWritable>> emitter) throws Exception {
    // Create StructuredRecord that only has the columns in this.columns
    List<Schema.Field> outputFields = new ArrayList<>();
    for (String column : columns) {
      Schema.Field field = input.getSchema().getField(column);
      Preconditions.checkNotNull(field, "Missing schema field for column '%s'", column);
      outputFields.add(field);
    }
    StructuredRecord.Builder output = StructuredRecord.builder(
      Schema.recordOf(input.getSchema().getRecordName(), outputFields));
    for (String column : columns) {
      output.set(column, input.get(column));
    }

    emitter.emit(new KeyValue<DBRecord, NullWritable>(new DBRecord(output.build(), columnTypes), null));
  }

  public static DriverCleanup ensureJDBCDriverIsAvailable(Class<? extends Driver> jdbcDriverClass,
                                                          String connectionString, String jdbcPluginType,
                                                          String jdbcPluginName)
    throws IllegalAccessException, InstantiationException, SQLException {
    try {
      DriverManager.getDriver(connectionString);
      return null;
//      return new DriverCleanup((JDBCDriverShim)null);
    } catch (SQLException var8) {
      LOG.error("Got sql exception.", var8);
      LOG.debug("Plugin Type: {} and Plugin Name: {}; Driver Class: {} not found. Registering JDBC driver via shim {} ",
                new Object[]{jdbcPluginType, jdbcPluginName,
                  jdbcDriverClass.getName(), JDBCDriverShim.class.getName()});
      JDBCDriverShim driverShim = new JDBCDriverShim((Driver)jdbcDriverClass.newInstance());

//      try {
//        deregisterAllDrivers(jdbcDriverClass);
//      } catch (ClassNotFoundException | NoSuchFieldException var7) {
//        LOG.error("Unable to deregister JDBC Driver class {}", jdbcDriverClass);
//      }

      DriverManager.registerDriver(driverShim);
//      return new DriverCleanup(driverShim);
      return null;
    }
  }

  private void setResultSetMetadata() throws Exception {
    Map<String, Integer> columnToType = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    ensureJDBCDriverIsAvailable(driverClass, config.connectionString, config.jdbcPluginType, config.jdbcPluginName);
//    dbManager.ensureJDBCDriverIsAvailable(driverClass);

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

    @Description("Table to write to.")
    @Macro
    private String tableName;

    @Description("Comma-separated list of columns.")
    @Macro
    private String columns;
  }
}
