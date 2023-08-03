package org.apache.pinot.segment.local.function;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.reflections.Reflections;

public class BiFunctionEvaluatorFactory {
    private static final Map<String, Class<? extends BiFunctionEvaluator>> IMPLEMENTATION_CLASSES = new HashMap<>();

    // Initialize the factory by scanning the classpath for implementations
    static {
        Reflections reflections = new Reflections("org.apache.pinot.segment.local.function");
        Set<Class<? extends BiFunctionEvaluator>> subTypes = reflections.getSubTypesOf(BiFunctionEvaluator.class);
        for (Class<? extends BiFunctionEvaluator> implementationClass : subTypes) {
            IMPLEMENTATION_CLASSES.put(implementationClass.getSimpleName(), implementationClass);
        }
    }

    public static BiFunctionEvaluator getInstance(String implementationClassName) throws Exception {
        Class<? extends BiFunctionEvaluator> implementationClass = IMPLEMENTATION_CLASSES.get(implementationClassName);
        if (implementationClass == null) {
            throw new IllegalArgumentException("Unknown implementation class: " + implementationClassName);
        }

        // Use reflection to create an instance of the implementation class
        return implementationClass.getDeclaredConstructor().newInstance();
    }

    public static Map<String, Class<? extends BiFunctionEvaluator>> getImplementationClasses() {
        return IMPLEMENTATION_CLASSES;
    }
}
