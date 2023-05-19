package com.swad.datalink.context;

import com.swad.datalink.asserts.Asserts;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

/**
 * @author: ruic
 * @data: 2023/5/19
 * @description:
 */
public class JarPathContextHolder {

    private static final ThreadLocal<Set<File>> UDF = new ThreadLocal<>();
    private static final ThreadLocal<Set<File>> OTHER = new ThreadLocal<>();

    public static void addUdfPath(File file) {
        if (Asserts.isNull(UDF.get())) {
            UDF.set(new HashSet<>());
        }
        UDF.get().add(file);
    }

    public static void addOtherPlugin(File file) {
        if (Asserts.isNull(OTHER.get())) {
            OTHER.set(new HashSet<>());
        }
        OTHER.get().add(file);
    }

    public static Set<File> getUdfFiles() {
        if (Asserts.isNull(UDF.get())) {
            UDF.set(new HashSet<>());
        }
        return UDF.get();
    }

    public static Set<File> getOtherPluginFiles() {
        if (Asserts.isNull(OTHER.get())) {
            OTHER.set(new HashSet<>());
        }
        return OTHER.get();
    }
}
