package com.swad.datalink.classloader;

import com.swad.datalink.exception.RunTimeException;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLStreamHandlerFactory;
import java.util.Collection;

/**
 * @author: ruic
 * @data: 2023/5/19
 * @description:
 */
public class DatalinkClassloader extends URLClassLoader {

    public DatalinkClassloader(Collection<File> fileSet, ClassLoader parent) {
        super(new URL[]{}, parent);
        fileSet.stream().map(x -> {
            try {
                return x.toURI().toURL();
            } catch (MalformedURLException e) {
                throw new RunTimeException(e.getMessage());
            }
        }).forEach(super::addURL);
    }
}
