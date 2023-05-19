package com.swad.datalink.job;

import com.swad.datalink.asserts.Asserts;
import com.swad.datalink.classloader.DatalinkClassloader;
import com.swad.datalink.context.ClassloaderContextHolder;
import com.swad.datalink.context.JarPathContextHolder;
import com.swad.datalink.exception.RunTimeException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.flink.configuration.PipelineOptions;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

/**
 * @author: ruic
 * @data: 2023/5/18
 * @description:
 */
@Slf4j
public class JobManager {
    private JobConfig jobConfig;

    public void executeSql(String statement) {
        initClassLoader();
    }

    private void initClassLoader() {
        if (Asserts.isNullMap(jobConfig.getConfig())) {
            String pipelineJars = jobConfig.getConfig().get(PipelineOptions.JARS.key());
            String classpaths = jobConfig.getConfig().get(PipelineOptions.CLASSPATHS.key());
            if (!Asserts.isNullString(pipelineJars)) {
                String[] jars = pipelineJars.split(",");
                for (String path: jars) {
                    File file = FileUtils.getFile(path);
                    if (!file.exists()) {
                        throw new RunTimeException("file: "+ path + " not exists");
                    }
                    JarPathContextHolder.addUdfPath(file);
                }
            }

            if (!Asserts.isNotNullString(classpaths)) {
                String[] paths = classpaths.split(",");
                for (String path: paths) {
                    File file = FileUtils.getFile(path);
                    if (!file.exists()) {
                        throw new RunTimeException("file: "+ path + " not exists");
                    }
                    JarPathContextHolder.addOtherPlugin(file);
                }
            }
        }
        Set<File> all = new HashSet<>();
        all.addAll(JarPathContextHolder.getUdfFiles());
        all.addAll(JarPathContextHolder.getOtherPluginFiles());
        DatalinkClassloader classloader = new DatalinkClassloader(all, Thread.currentThread().getContextClassLoader());
        ClassloaderContextHolder.set(classloader);
    }
}
