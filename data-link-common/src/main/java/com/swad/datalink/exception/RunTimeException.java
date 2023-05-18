package com.swad.datalink.exception;

/**
 * @author: ruic
 * @data: 2023/5/12
 * @description:
 */
public class RunTimeException extends RuntimeException {
    public RunTimeException(String message, Throwable cause) {
        super(message, cause);
    }

    public RunTimeException(String message) {
        super(message);
    }
}
