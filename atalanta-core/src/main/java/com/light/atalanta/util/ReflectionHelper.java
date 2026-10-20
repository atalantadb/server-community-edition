/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;

public final class ReflectionHelper {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReflectionHelper.class);

    public static void setInnerField(Object instance, String fieldName, Object value) {
        try {
            Class<?> iClass = instance.getClass();
            while (iClass != null) {
                Field[] fields = iClass.getDeclaredFields();
                for (Field f : fields) {
                    if (f.getName().equals(fieldName)) {
                        f.setAccessible(true);
                        f.set(instance, value);
                        return;
                    }
                }
                iClass = iClass.getSuperclass();
            }
            LOGGER.warn("Field {} is  not found", fieldName);
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    public static void setInnerClassField(Class instance, String fieldName, Object value) {
        try {
            Field f = instance.getDeclaredField(fieldName);
            f.setAccessible(true);
            f.set(instance, value);
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> T getInnerFieldValue(Object instance, String fieldName) {
        try {
            Field findField = null;
            Class<?> iClass = instance.getClass();
            while (iClass != null) {
                Field[] fields = iClass.getDeclaredFields();
                for (Field f : fields) {
                    if (f.getName().equals(fieldName)) {
                        findField = f;
                        f.setAccessible(true);
                        break;
                    }
                }
                if (findField != null) {
                    break;
                }
                iClass = iClass.getSuperclass();
            }
            if (findField == null) {
                LOGGER.warn("Field {} is not found", fieldName);
                return null;
            }
            return (T) findField.get(instance);
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    static <T> T getStaticInnerClassValue(Class<?> instance, String fieldName) {
        try {
            Field f = instance.getDeclaredField(fieldName);
            f.setAccessible(true);
            return (T) f.get(instance);
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
        return null;
    }

    public static <T> void changeInnerFinalVariable(Class cl, String variableName, T value) {
        try {
            Field field = cl.getDeclaredField(variableName);
            field.setAccessible(true);

            Field modifiersField = Field.class.getDeclaredField("modifiers");
            modifiersField.setAccessible(true);
            modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);

            field.set(null, value);
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public static <T> void changeInnerFinalVariable(Object object, String variableName, T value) {
        try {
            Field field = object.getClass().getDeclaredField(variableName);
            field.setAccessible(true);

            Field modifiersField = Field.class.getDeclaredField("modifiers");
            modifiersField.setAccessible(true);
            modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);

            field.set(object, value);
            System.out.println();
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public static void addEnvironment(String key, String value) {
        getEnvAsMap().put(key, value);
    }

    public static void removeEnvironment(String key) {
        getEnvAsMap().remove(key);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, String> getEnvAsMap() {
        try {
            Class<?> pe = Class.forName("java.lang.ProcessEnvironment");
            Method getEnv = pe.getDeclaredMethod("getenv");
            getEnv.setAccessible(true);

            Object unmodifiableMap = getEnv.invoke(null);
            Class<?> map = Class.forName("java.util.Collections$UnmodifiableMap");
            Field m = map.getDeclaredField("m");
            m.setAccessible(true);
            return (Map<String, String>) m.get(unmodifiableMap);
        } catch (Exception e) {
            e.printStackTrace();
            return new HashMap<>();
        }
    }

    private ReflectionHelper() {
    }
}

