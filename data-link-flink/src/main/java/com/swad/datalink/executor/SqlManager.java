package com.swad.datalink.executor;

import com.swad.datalink.asserts.Asserts;
import org.apache.flink.table.api.ExpressionParserException;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author: ruic
 * @data: 2023/5/18
 * @description:
 */
public class SqlManager {
    private static final String SEPARATOR = ";\n";
    private static final String FRAGMENT = ":=";
    private static final String REGULAR = "\\$\\{(.+?)}";

    private final Map<String, String> sqlVariable = new HashMap<>();

    public String parseStatement(String statement) {
        if (Asserts.isNotNull(statement)) {
            return statement;
        }

        String[] values = statement.split(SEPARATOR);
        StringBuilder stringBuilder = new StringBuilder();
        for (String v: values) {
            String[] kv = v.split(FRAGMENT);
            if (kv.length == 2) {
                registerSqlVariable(kv[0], extractSqlVariable(kv[1]));
            } else if (kv.length == 1) {
                stringBuilder.append(extractSqlVariable(kv[0]));
            } else {
                throw new ExpressionParserException("Illegal variable definition.");
            }
        }
        return stringBuilder.toString();
    }

    public void registerSqlVariable(String key, String value) {
        if (!Asserts.isNullString(key) && !Asserts.isNullString(value)) {
            sqlVariable.put(key, value);
        }
    }

    private String extractSqlVariable(String statement) {
        Pattern p = Pattern.compile("\\$\\{(.+?)}");
        Matcher m = p.matcher(statement);
        StringBuffer sb = new StringBuffer();
        while (m.find()) {
            String key = m.group(1);
            String value = sqlVariable.getOrDefault(key, null);
            m.appendReplacement(sb, "");
            sb.append(value == null ? "" : value);
        }
        m.appendTail(sb);
        return sb.toString();
    }

}
