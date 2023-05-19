package com.swad.datalink.job;

/**
 * @author: ruic
 * @data: 2023/5/19
 * @description:
 */
public class Job {
    private Integer id;
    private JobStatus status;

    public enum JobStatus {
        INIT, RUNNING, SUCCESS, FAILED, CANCEL
    }
}
