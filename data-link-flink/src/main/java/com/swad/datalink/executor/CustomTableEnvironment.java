package com.swad.datalink.executor;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.JSONGenerator;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.bridge.internal.AbstractStreamTableEnvironmentImpl;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.delegation.ExecutorFactory;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.PlannerFactoryUtil;
import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.resource.ResourceManager;
import org.apache.flink.util.FlinkUserCodeClassLoaders;
import org.apache.flink.util.MutableURLClassLoader;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class CustomTableEnvironment extends AbstractStreamTableEnvironmentImpl {
    public CustomTableEnvironment(CatalogManager catalogManager, ModuleManager moduleManager, FunctionCatalog functionCatalog,
                                  ResourceManager resourceManager, TableConfig tableConfig, StreamExecutionEnvironment executionEnvironment,
                                  Planner planner, Executor executor, boolean isStreamingMode) {
        super(catalogManager, moduleManager, resourceManager, tableConfig, executor, functionCatalog, planner, isStreamingMode, executionEnvironment);
    }

    public static CustomTableEnvironment create(StreamExecutionEnvironment executionEnvironment) {
        return create(executionEnvironment, EnvironmentSettings.newInstance().inStreamingMode().build());
    }

    public static CustomTableEnvironment createBatch(StreamExecutionEnvironment executionEnvironment) {
        return create(executionEnvironment, EnvironmentSettings.newInstance().inBatchMode().build());
    }

    public static CustomTableEnvironment create(StreamExecutionEnvironment executionEnvironment, EnvironmentSettings settings) {
        MutableURLClassLoader userClassLoader = FlinkUserCodeClassLoaders.create(new URL[0], settings.getUserClassLoader(), settings.getConfiguration());
        ExecutorFactory executorFactory = (ExecutorFactory) FactoryUtil.discoverFactory(userClassLoader, ExecutorFactory.class, "default");
        Executor executor = executorFactory.create(settings.getConfiguration());
        TableConfig tableConfig = TableConfig.getDefault();
        tableConfig.setRootConfiguration(executor.getConfiguration());
        tableConfig.addConfiguration(settings.getConfiguration());
        ResourceManager resourceManager = new ResourceManager(settings.getConfiguration(), userClassLoader);
        ModuleManager moduleManager = new ModuleManager();
        CatalogManager catalogManager = CatalogManager.newBuilder().classLoader(userClassLoader).config(tableConfig).defaultCatalog(settings.getBuiltInCatalogName(), new GenericInMemoryCatalog(settings.getBuiltInCatalogName(), settings.getBuiltInDatabaseName())).build();
        FunctionCatalog functionCatalog = new FunctionCatalog(tableConfig, resourceManager, catalogManager, moduleManager);
        Planner planner = PlannerFactoryUtil.createPlanner(executor, tableConfig, userClassLoader, moduleManager, catalogManager, functionCatalog);
        return new CustomTableEnvironment(catalogManager, moduleManager, functionCatalog, resourceManager, tableConfig, executionEnvironment, planner, executor, settings.isStreamingMode());
    }

    public ObjectNode getStreamGraph(String statement) {
        List<Operation> operations = super.getParser().parse(statement);
        if (operations.size() != 1) {
            throw new TableException("Unsupported SQL query! explainSql() only accepts a single SQL query.");
        }
        ArrayList<ModifyOperation> modifyOperation = new ArrayList<>();
        operations.forEach(op -> {
            if (op instanceof ModifyOperation) {
                modifyOperation.add((ModifyOperation) op);
            }
        });
        List<Transformation<?>> translates = super.planner.translate(modifyOperation);
        for (Transformation<?> trans: translates) {
            executionEnvironment.addOperator(trans);
        }
        StreamGraph graph = executionEnvironment.getStreamGraph();
        if (tableConfig.getConfiguration().containsKey(PipelineOptions.NAME.key())) {
            graph.setJobName(tableConfig.getConfiguration().getString(PipelineOptions.NAME));
        }
        JSONGenerator jsonGenerator = new JSONGenerator(graph);
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode objectNode = objectMapper.createObjectNode();
        try {
            objectNode = (ObjectNode) objectMapper.readTree(jsonGenerator.getJSON());
        } catch (JacksonException e) {
            e.printStackTrace();
        }
        return objectNode;
    }


}
