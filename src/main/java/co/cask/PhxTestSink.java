package co.cask;


import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.FileSetArguments;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.hydrator.plugin.DBRecord;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("phxSink")
@Description("Add description here")
//public class PhxTestSink extends ReferenceBatchSink<StructuredRecord, DBRecord, NullWritable> {
public class PhxTestSink extends BatchSink<StructuredRecord, NullWritable, Text> {
    public static final String NAME = "TextFileSet";
    private final Conf config;

    /**
     * Config properties for the plugin.
     */
    public static class Conf extends PluginConfig {
        public static final String outputTable = "outputTable";
        public static final String outputColumns = "outputColumns";

        // The name annotation tells CDAP what the property name is. It is optional, and defaults to the variable name.
        // Note:  only primitives (including boxed types) and string are the types that are supported
        @Name(outputTable)
        @Description("The name of the table to write to.")
        private String tableName;

        // Macro enabled properties can be set to a placeholder value ${key} when the pipeline is deployed.
        // At runtime, the value for 'key' can be given and substituted in.
        @Macro
        @Name(outputColumns)
        @Description("the columns write to.")
        private String columns;


        // Use a no-args constructor to set field defaults.
        public Conf() {
            tableName = "";
        }
    }

    // CDAP will pass in a config with its fields populated based on the configuration given when creating the pipeline.
    public PhxTestSink(Conf config) {
        this.config = config;
    }

    // configurePipeline is called exactly once when the pipeline is being created.
    // Any static configuration should be performed here.
    @Override
    public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
        // create the FileSet here.
        pipelineConfigurer.createDataset(config.columns,
                FileSet.class,
                FileSetProperties.builder()
                        .setInputFormat(TextInputFormat.class)
                        .setOutputFormat(TextOutputFormat.class)
                        .setEnableExploreOnCreate(true)
                        .setExploreFormat("text")
                        .setExploreSchema("text string")
                        .build()
        );
    }

    // prepareRun is called before every pipeline run, and is used to configure what the input should be,
    // as well as any arguments the input should use. It is called by the client that is submitting the batch job.
    @Override
    public void prepareRun(BatchSinkContext context) throws Exception {
        Map<String, String> arguments = new HashMap<>();
        FileSetArguments.setOutputPath(arguments, config.tableName);
        context.addOutput(Output.ofDataset(config.columns, arguments));
    }

    // onRunFinish is called at the end of the pipeline run by the client that submitted the batch job.
    @Override
    public void onRunFinish(boolean succeeded, BatchSinkContext context) {
        // perform any actions that should happen at the end of the run.
    }

    // initialize is called by each job executor before any call to transform is made.
    // This occurs at the start of the batch job run, after the job has been successfully submitted.
    // For example, if mapreduce is the execution engine, each mapper will call initialize at the start of the program.
    @Override
    public void initialize(BatchRuntimeContext context) throws Exception {
        super.initialize(context);
        // create any resources required by transform()
    }

    // destroy is called by each job executor at the end of its life.
    // For example, if mapreduce is the execution engine, each mapper will call destroy at the end of the program.
    @Override
    public void destroy() {
        // clean up any resources created by initialize
    }

    @Override
//    public void transform(StructuredRecord input, Emitter<KeyValue<NullWritable, Text>> emitter) throws Exception {
    public void transform(StructuredRecord input, Emitter<KeyValue<NullWritable, DBRecord>> emitter) throws Exception {

            StringBuilder joinedFields = new StringBuilder();
        Iterator<Schema.Field> fieldIter = input.getSchema().getFields().iterator();
        if (!fieldIter.hasNext()) {
            // shouldn't happen
            return;
        }

        Object val = input.get(fieldIter.next().getName());
        if (val != null) {
            joinedFields.append(val);
        }
        while (fieldIter.hasNext()) {
            String fieldName = fieldIter.next().getName();
//            joinedFields.append(config.fieldSeparator);
            val = input.get(fieldName);
            if (val != null) {
                joinedFields.append(val);
            }
        }
        emitter.emit(new KeyValue<>(NullWritable.get(), new Text(joinedFields.toString())));
    }

}