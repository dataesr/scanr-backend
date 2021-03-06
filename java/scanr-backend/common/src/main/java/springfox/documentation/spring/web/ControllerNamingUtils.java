/*
 * Copyright 2016-2019 MESRI
 * Apache License 2.0
 */
package springfox.documentation.spring.web;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;
import org.springframework.web.method.HandlerMethod;
import org.springframework.web.util.UriUtils;

import java.io.UnsupportedEncodingException;

import static springfox.documentation.spring.web.paths.Paths.splitCamelCase;

// OVERRIDING THE SPRINGFOX BEHAVIOUR!
// THIS IS HACKY AND ALLOWS TO OVERRIDE TAG GENERATION (meaning api group identification)
public class ControllerNamingUtils {
    private static Logger log = LoggerFactory.getLogger(ControllerNamingUtils.class);
    private static final String ISO_8859_1 = "ISO-8859-1";

    public static String pathRoot(String requestPattern) {
        Assert.notNull(requestPattern);
        Assert.hasText(requestPattern);
        log.info("Resolving path root for {}", requestPattern);
        requestPattern = requestPattern.startsWith("/") ? requestPattern : "/" + requestPattern;
        int idx = requestPattern.indexOf("/", 1);
        if (idx > -1) {
            return requestPattern.substring(0, idx);
        }
        return requestPattern;
    }

    public static String pathRootEncoded(String requestPattern) {
        return encode(pathRoot(requestPattern));
    }

    public static String encode(String path) {
        try {
            path = UriUtils.encodePath(path, ISO_8859_1);
        } catch (UnsupportedEncodingException e) {
            log.error("Could not encode:" + path, e);
        }
        return path;
    }

    public static String decode(String path) {
        try {
            path = UriUtils.decode(path, ISO_8859_1);
        } catch (Exception e) {
            log.error("Could not decode:" + path, e);
        }
        return path;
    }


    public static String controllerNameAsGroup(HandlerMethod handlerMethod) {
        Class<?> controllerClass = handlerMethod.getBeanType();
        // THIS IS THE CHANGE, REPLACE "-" BY " " AND REMOVE THE API SUFFIX
        return splitCamelCase(controllerClass.getSimpleName(), " ").replaceFirst(" Api$", "")
                .replace("/", "")
                .toLowerCase();
    }
}
