package com.dcs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.stream.IntStream;


class DcsDict {

    static Logger LOGGER = LoggerFactory.getLogger("ObjDict");

    void setValue(String key, Object val) {
        try {
            String field_key = key.equals("group") ? "grp" : key;
            Class cls = this.getClass();
            Field field = cls.getDeclaredField(field_key);
            field.setAccessible(true);
            field.set(this, val);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            e.printStackTrace();
            LOGGER.error("Field not found: " + key);
        }
    }

    void print() {
        Field[] fields = this.getClass().getFields();
        IntStream.range(0, fields.length).forEach(i -> {
            try {
                LOGGER.debug(fields[i].toString() + ": " + fields[i].get(this));
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        });
    }

    HashMap<String, Object> toHashMap() {
        HashMap<String, Object> map = new HashMap<>();
        Field[] fields = this.getClass().getFields();
        Arrays.stream(fields)
                .forEach(field -> {
                    try {
                        if (field.get(this) != null) {
                            map.put(field.getName(), field.get(this));
                        }
                    } catch (IllegalAccessException e) {
                        System.out.println("error");
                    }
                });
        return map;
    }
}
