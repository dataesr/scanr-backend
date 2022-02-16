/*
 * Copyright 2016-2019 MESRI
 * Apache License 2.0
 */
package fr.gouv.recherche.scanr.db.model.full;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.google.common.collect.Maps;

import fr.gouv.recherche.scanr.db.model.full.FullStructure;
import fr.gouv.recherche.scanr.db.model.full.FullStructureField;
import org.apache.commons.lang3.text.WordUtils;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * Technical class to define the different full structure fields to compute in the workflow.
 */
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public enum FullStructureField {

    PARENTS,
    CHILDREN,
    PREDECESSORS,
    RELATIONS,
    PROJECTS,
    PUBLICATIONS,
    PERSONS,
    LEADERS,
    GRAPH,
    WEBSITES,
    SPINOFF_FROM,
    INSTITUTIONS;

    private static final Map<FullStructureField, BiConsumer<FullStructure, Object>> setters = Maps.newHashMap();
    private static final Map<FullStructureField, Function<FullStructure, Object>> getters = Maps.newHashMap();

    static {
        for (Method method : FullStructure.class.getMethods()) {
            String name = method.getName();
            name = name.replaceAll("([a-z])([A-Z])", "$1_$2");
            name = name.toUpperCase();
            if (!name.startsWith("SET_") && !name.startsWith("GET_")) {
                continue;
            }
            String attrName = name.substring(4);
            if (attrName.equals("ID") || attrName.equals("CLASS") || attrName.equals("LAST_UPDATED") || attrName.equals("INDEXED") || attrName.equals("FIELDS_TO_REFRESH"))
                continue;
            FullStructureField key = null;
            try {
                key = FullStructureField.valueOf(attrName);
            } catch (IllegalArgumentException e) {
                // discard unkown fields
                continue;
            }

            if (name.startsWith("SET_")) {
                setters.put(key, (it, data) -> {
                    try {
                        method.invoke(it, data);
                    } catch (Exception e) {
                        throw new IllegalStateException(e);
                    }
                });
            } else if (name.startsWith("GET_")) {
                getters.put(key, it -> {
                    try {
                        return method.invoke(it);
                    } catch (Exception e) {
                        throw new IllegalStateException(e);
                    }
                });
            }
        }
    }

    public Function<FullStructure, Object> getter() {
        return getters.get(this);
    }

    public BiConsumer<FullStructure, Object> setter() {
        return setters.get(this);
    }

    private static final char[] UNDERSCORE_DELIMITER = new char[]{'_'};

    public String toAttributeName() {
        return WordUtils.uncapitalize(WordUtils.capitalize(name().toLowerCase(), UNDERSCORE_DELIMITER).replace("_", ""));
    }
}
