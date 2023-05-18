package com.swad.datalink.executor;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

/**
 * @author: ruic
 * @data: 2023/5/12
 * @description:
 */
@Slf4j
@Getter
public class ExecutorSetting {
    private boolean useBatchModel;
    private Map<String, String> config;
    private Integer parallelism;
    private String jobName;
}
