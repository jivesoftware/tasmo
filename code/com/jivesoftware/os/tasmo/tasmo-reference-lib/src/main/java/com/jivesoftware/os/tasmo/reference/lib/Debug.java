package com.jivesoftware.os.tasmo.reference.lib;

/**
 *
 * @author jonathan
 */
public class Debug {

    public static String caller(int depth) {
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        StringBuilder sb = new StringBuilder();
        for (int i = Math.min(stackTrace.length - 3, depth - 1); i > -1; i--) {
            StackTraceElement e = stackTrace[3 + i];
            String className = e.getClassName();
            className = className.substring(className.lastIndexOf("."));
            sb.append(className).append(".").append(e.getMethodName()).append(":").append(e.getLineNumber()).append("->");
        }
        return sb.toString();
    }
}
