package com.swad.datalink.context;

import com.swad.datalink.classloader.DatalinkClassloader;
import com.swad.datalink.exception.RunTimeException;

import java.io.IOException;

/**
 * @author: ruic
 * @data: 2023/5/19
 * @description:
 */
public class ClassloaderContextHolder {
    private static final ThreadLocal<DatalinkClassloader> CLASS_LOADER = new ThreadLocal<>();
    private static final ThreadLocal<ClassLoader> INIT_CLASS_LOADER = new ThreadLocal<>();

    public static void set(DatalinkClassloader classloader) {
        INIT_CLASS_LOADER.set(Thread.currentThread().getContextClassLoader());
        CLASS_LOADER.set(classloader);
        Thread.currentThread().setContextClassLoader(classloader);
    }

    public static DatalinkClassloader get() {
        return CLASS_LOADER.get();
    }

    public static void clear() {
        DatalinkClassloader datalinkClassloader = get();
        CLASS_LOADER.remove();
        try {
            datalinkClassloader.close();
        } catch (IOException e) {
            throw new RunTimeException(e.getMessage());
        }
        Thread.currentThread().setContextClassLoader(INIT_CLASS_LOADER.get());
        INIT_CLASS_LOADER.remove();
    }
}
