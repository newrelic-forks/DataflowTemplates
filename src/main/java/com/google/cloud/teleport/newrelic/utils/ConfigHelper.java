package com.google.cloud.teleport.newrelic.utils;

import org.apache.beam.sdk.options.ValueProvider;

public class ConfigHelper {
    private ConfigHelper() {
        throw new AssertionError("This class should not be instantiated");
    }

    public static <T> T valueOrDefault(ValueProvider<T> value, T defaultValue ) {
        return (value != null && value.isAccessible()) && value.get() != null
                ? value.get()
                : defaultValue;
    }
}
