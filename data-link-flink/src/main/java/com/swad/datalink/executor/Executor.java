package com.swad.datalink.executor;

import com.swad.datalink.asserts.Asserts;
import com.swad.datalink.utils.SqlUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;

import java.util.Map;

/**
 * @author: ruic
 * @data: 2023/5/12
 * @description:
 */
@Slf4j
public class Executor {
    private ExecutorSetting executorSetting;
    private StreamExecutionEnvironment environment;
    private CustomTableEnvironment customEnvironment;

    private final SqlManager sqlManager = new SqlManager();

    public static Executor buildExecutor(ExecutorSetting executorSetting) {
        StreamExecutionEnvironment environment;
        if (Asserts.isNotNull(executorSetting.getConfig())) {
            Configuration configuration = Configuration.fromMap(executorSetting.getConfig());
            environment = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        } else {
            environment = StreamExecutionEnvironment.getExecutionEnvironment();
        }
        return new Executor(executorSetting, environment);
    }

    public JobExecutionResult execute(String jobName) throws Exception {
        return environment.execute(jobName);
    }

    public TableResult executeSql(String statement) {
        statement = SqlUtil.removeNote(statement);
        statement = sqlManager.parseStatement(statement).trim();
        return customEnvironment.executeSql(statement);
    }

    private Executor(ExecutorSetting executorSetting, StreamExecutionEnvironment environment) {
        this.executorSetting = executorSetting;
        this.environment = environment;

        updateEnvironment(executorSetting);
        updateStreamExecutionEnvironment(executorSetting);
    }

    private void updateEnvironment(ExecutorSetting executorSetting) {
        environment.setParallelism(executorSetting.getParallelism());
        if (Asserts.isNotNull(executorSetting.getConfig())) {
            Configuration configuration = Configuration.fromMap(executorSetting.getConfig());
            environment.getConfig().configure(configuration, null);
        }
    }

    private void updateStreamExecutionEnvironment(ExecutorSetting executorSetting) {
        CustomTableEnvironment newestEnvironment = createCustomTableEnvironment();
        if (Asserts.isNotNull(customEnvironment)) {
            for (String name: customEnvironment.listCatalogs()) {
                customEnvironment.getCatalog(name).ifPresent(c -> {
                    newestEnvironment.getCatalogManager().unregisterCatalog(name, true);
                    newestEnvironment.registerCatalog(name, c);
                });
            }
        }
        customEnvironment = newestEnvironment;
        Configuration configuration = customEnvironment.getConfig().getConfiguration();
        configuration.setString(PipelineOptions.NAME.key(), executorSetting.getJobName());
        if (Asserts.isNotNull(executorSetting.getConfig())) {
            for (Map.Entry<String, String> entry : executorSetting.getConfig().entrySet()) {
                configuration.setString(entry.getKey(), entry.getValue());
            }
        }
    }

    private CustomTableEnvironment createCustomTableEnvironment() {
        if (executorSetting.isUseBatchModel()) {
            return CustomTableEnvironment.createBatch(environment);
        } else {
            return CustomTableEnvironment.create(environment);
        }
    }
}
