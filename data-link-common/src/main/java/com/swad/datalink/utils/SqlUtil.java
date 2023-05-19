package com.swad.datalink.utils;

import com.swad.datalink.asserts.Asserts;

/**
 * @author: ruic
 * @data: 2023/5/18
 * @description:
 */
public class SqlUtil {
    public static String removeNote(String sql) {
        if (Asserts.isNotNullString(sql)) {
            sql = sql.replaceAll("\u00A0", " ")
                    .replaceAll("[\r\n]+", "\n")
                    .replaceAll("--([^'\n]{0,}('[^'\n]{0,}'){0,1}[^'\n]{0,}){0,}", "").trim();
        }
        return sql;
    }
}
