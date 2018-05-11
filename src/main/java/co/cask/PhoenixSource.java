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
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.plugin.EndpointPluginContext;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.api.plugin.PluginProperties;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.hydrator.common.SourceInputFormatProvider;
import co.cask.hydrator.common.batch.JobUtils;
import co.cask.hydrator.plugin.*;
import co.cask.hydrator.plugin.DBUtils;
import co.cask.hydrator.plugin.db.batch.source.DBSource;


import co.cask.hydrator.plugin.DBConfig;
import co.cask.hydrator.plugin.DBManager;
import co.cask.hydrator.plugin.DBRecord;
import co.cask.hydrator.plugin.DBUtils;
import co.cask.hydrator.plugin.DriverCleanup;
import co.cask.hydrator.plugin.FieldCase;
import co.cask.hydrator.plugin.StructuredRecordUtils;


import co.cask.hydrator.plugin.db.batch.source.DataDrivenETLDBInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.phoenix.mapreduce.PhoenixInputFormat;
import org.apache.phoenix.mapreduce.PhoenixRecordReader;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.mapreduce.util.PhoenixMapReduceUtil;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.phoenix.util.ColumnInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import javax.annotation.Nullable;
import javax.ws.rs.Path;
import java.io.IOException;
import java.sql.*;

/**
 * A source that uses the {@link PhoenixMapReduceUtil} to read data from Phoenix tables.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name("PhoenixSource")
@Description("Reads content of an HBase table using Phoenix")
public class PhoenixSource extends BatchSource<LongWritable, DBRecord, StructuredRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(PhoenixSource.class);

  private final Config config;
  private final Schema outputSchema;

  public PhoenixSource(Config config) {
    this.config = config;
    this.outputSchema = createOutputSchema();
  }

  @Override
  public void configurePipeline(PipelineConfigurer configurer) {
    configurer.getStageConfigurer().setOutputSchema(outputSchema);
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    Job job = JobUtils.createInstance();
    PhoenixMapReduceUtil.setInput(job, DBRecord.class, config.tableName, config.inputQuery);
    context.setInput(Input.of(config.referenceName,
                              new SourceInputFormatProvider(PhoenixInputFormat.class, job.getConfiguration())));
  }

//  @Override
//  public void transform(KeyValue<LongWritable, DBRecord> input, Emitter<StructuredRecord> emitter) throws Exception {
//    emitter.emit(StructuredRecordUtils.convertCase(
//            input.getValue().getRecord(), FieldCase.toFieldCase(sourceConfig.columnNameCase)));
//  }
  @Override
  public void transform(KeyValue<LongWritable, DBRecord> input, Emitter<StructuredRecord> emitter) throws Exception {
      emitter.emit(input.getValue().getRecord());
  }

  private Schema createOutputSchema() {
    return Schema.recordOf("output"
//            ,
//            Schema.Field.of("filePath", Schema.of(Schema.Type.STRING)),
//            Schema.Field.of("body", Schema.of(Schema.Type.BYTES))
    );
  }

  /**
   * Configurations for the {@link PhoenixSource} plugin.
   */
  public static final class Config extends PluginConfig {

    @Description(
      "This will be used to uniquely identify this source/sink for lineage, annotating metadata, etc."
    )
    private String referenceName;

    @Description(
      "Table to read from."
    )
    @Macro
    private String tableName;

    @Description(
      "The SELECT query to use to import data from the specified table."
    )
    @Macro
    private String inputQuery;

    @Override
    public String toString() {
      return "Config{" +
        "referenceName='" + referenceName + '\'' +
        ", tableName='" + tableName + '\'' +
        ", inputQuery='" + inputQuery + '\'' +
        '}';
    }
  }



/**
 *
 *  ALL the Database (JDBC) specific stuff goes here
 *
 *
 *
**/


class GetSchemaRequest {
    public String connectionString;
    @Nullable
    public String user;
    @Nullable
    public String password;
    public String jdbcPluginName;
    @Nullable
    public String jdbcPluginType;
    public String query;

    private String getJDBCPluginType() {
        return jdbcPluginType == null ? "jdbc" : jdbcPluginType;
    }
}

    /**
     * Endpoint method to get the output schema of a query.
     *
     * @param request {@link DBSource.GetSchemaRequest} containing information required for connection and query to execute.
     * @param pluginContext context to create plugins
     * @return schema of fields
     * @throws SQLException
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    @Path("getSchema")
    public Schema getSchema(GetSchemaRequest request,
                            EndpointPluginContext pluginContext) throws IllegalAccessException,
            SQLException, InstantiationException {
        DriverCleanup driverCleanup;
        try {
            driverCleanup = loadPluginClassAndGetDriver(request, pluginContext);
            try (Connection connection = getConnection(request.connectionString, request.user, request.password)) {
                String query = request.query;
                Statement statement = connection.createStatement();
                statement.setMaxRows(1);
                if (query.contains("$CONDITIONS")) {
                    query = removeConditionsClause(query);
                }
                ResultSet resultSet = statement.executeQuery(query);
                return Schema.recordOf("outputSchema", DBUtils.getSchemaFields(resultSet));
            } finally {
                driverCleanup.destroy();
            }
        } catch (Exception e) {
            LOG.error("Exception while performing getSchema", e);
            throw e;
        }
    }

    private static String removeConditionsClause(String importQuerySring) {
        importQuerySring = importQuerySring.replaceAll("\\s{2,}", " ").toUpperCase();
        if (importQuerySring.contains("WHERE $CONDITIONS AND")) {
            importQuerySring = importQuerySring.replace("$CONDITIONS AND", "");
        } else if (importQuerySring.contains("WHERE $CONDITIONS")) {
            importQuerySring = importQuerySring.replace("WHERE $CONDITIONS", "");
        } else if (importQuerySring.contains("AND $CONDITIONS")) {
            importQuerySring = importQuerySring.replace("AND $CONDITIONS", "");
        }
        return importQuerySring;
    }

    private DriverCleanup loadPluginClassAndGetDriver(GetSchemaRequest request, EndpointPluginContext pluginContext)
            throws IllegalAccessException, InstantiationException, SQLException {
        Class<? extends Driver> driverClass =
                pluginContext.loadPluginClass(request.getJDBCPluginType(),
                        request.jdbcPluginName, PluginProperties.builder().build());

        if (driverClass == null) {
            throw new InstantiationException(
                    String.format("Unable to load Driver class with plugin type %s and plugin name %s",
                            request.getJDBCPluginType(), request.jdbcPluginName));
        }

        try {
            return DBUtils.ensureJDBCDriverIsAvailable(driverClass, request.connectionString,
                    request.getJDBCPluginType(), request.jdbcPluginName);
        } catch (IllegalAccessException | InstantiationException | SQLException e) {
            LOG.error("Unable to load or register driver {}", driverClass, e);
            throw e;
        }
    }

    private Connection getConnection(String connectionString,
                                     @Nullable String user, @Nullable String password) throws SQLException {
        if (user == null) {
            return DriverManager.getConnection(connectionString);
        } else {
            return DriverManager.getConnection(connectionString, user, password);
        }
    }

//    @Override
//    public void prepareRun(BatchSourceContext context) throws Exception {
//        sourceConfig.validate();
//
//        LOG.debug("pluginType = {}; pluginName = {}; connectionString = {}; importQuery = {}; " +
//                        "boundingQuery = {}",
//                sourceConfig.jdbcPluginType, sourceConfig.jdbcPluginName,
//                sourceConfig.connectionString, sourceConfig.getImportQuery(), sourceConfig.getBoundingQuery());
//        Configuration hConf = new Configuration();
//        hConf.clear();
//
//        // Load the plugin class to make sure it is available.
//        Class<? extends Driver> driverClass = context.loadPluginClass(getJDBCPluginId());
//        if (sourceConfig.user == null && sourceConfig.password == null) {
//            DBConfiguration.configureDB(hConf, driverClass.getName(), sourceConfig.connectionString);
//        } else {
//            DBConfiguration.configureDB(hConf, driverClass.getName(), sourceConfig.connectionString,
//                    sourceConfig.user, sourceConfig.password);
//        }
//        DataDrivenETLDBInputFormat.setInput(hConf, DBRecord.class,
//                sourceConfig.getImportQuery(), sourceConfig.getBoundingQuery(),
//                sourceConfig.getEnableAutoCommit());
//        if (sourceConfig.numSplits == null || sourceConfig.numSplits != 1) {
//            if (!sourceConfig.getImportQuery().contains("$CONDITIONS")) {
//                throw new IllegalArgumentException(String.format("Import Query %s must contain the string '$CONDITIONS'.",
//                        sourceConfig.importQuery));
//            }
//            hConf.set(DBConfiguration.INPUT_ORDER_BY_PROPERTY, sourceConfig.splitBy);
//        }
//        if (sourceConfig.numSplits != null) {
//            hConf.setInt(MRJobConfig.NUM_MAPS, sourceConfig.numSplits);
//        }
//        if (sourceConfig.schema != null) {
//            hConf.set(DBUtils.OVERRIDE_SCHEMA, sourceConfig.schema);
//        }
//        context.setInput(Input.of(sourceConfig.referenceName,
//                new SourceInputFormatProvider(DataDrivenETLDBInputFormat.class, hConf)));
//    }

}