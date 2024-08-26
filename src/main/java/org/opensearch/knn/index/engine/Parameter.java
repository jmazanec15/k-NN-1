/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.engine;

import lombok.Getter;
import org.opensearch.common.ValidationException;
import org.opensearch.knn.index.engine.validation.ValidationUtil;

import java.util.Locale;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Parameter that can be set for a method component
 *
 * @param <T> Type parameter takes
 */
public abstract class Parameter<T> {
    @Getter
    private final String name;
    protected final BiFunction<T, KNNIndexContext, ValidationException> resolver;
    protected final Function<T, ValidationException> validator;

    /**
     * Constructor
     *
     * @param name of the parameter
     * @param resolver resolves the parameter
     */
    public Parameter(
        String name,
        BiFunction<T, KNNIndexContext, ValidationException> resolver,
        Function<T, ValidationException> validator
    ) {
        this.name = name;
        this.resolver = resolver;
        this.validator = validator;
    }

    /**
     * Check if the value passed in is valid
     *
     * @param value to be checked
     * @return ValidationException produced by validation errors; null if no validations errors.
     */
    public abstract ValidationException resolve(Object value, KNNIndexContext knnIndexContext);

    public abstract ValidationException validate(Object value);

    /**
     * Boolean method parameter
     */
    public static class BooleanParameter extends Parameter<Boolean> {
        public BooleanParameter(
            String name,
            BiFunction<Boolean, KNNIndexContext, ValidationException> resolver,
            Function<Boolean, ValidationException> validator
        ) {
            super(name, resolver, validator);
        }

        @Override
        public ValidationException resolve(Object value, KNNIndexContext knnIndexContext) {
            ValidationException validationException = validate(value);
            if (validationException != null) return validationException;
            return resolver.apply((Boolean) value, knnIndexContext);
        }

        @Override
        public ValidationException validate(Object value) {
            if (value != null && !(value instanceof Boolean)) {
                ValidationException validationException = new ValidationException();
                validationException.addValidationError(
                    String.format("value is not an instance of Boolean for Boolean parameter [%s].", getName())
                );
                throw validationException;
            }
            return validator.apply((Boolean) value);
        }
    }

    /**
     * Integer method parameter
     */
    public static class IntegerParameter extends Parameter<Integer> {
        public IntegerParameter(
            String name,
            BiFunction<Integer, KNNIndexContext, ValidationException> resolver,
            Function<Integer, ValidationException> validator
        ) {
            super(name, resolver, validator);
        }

        @Override
        public ValidationException resolve(Object value, KNNIndexContext knnIndexContext) {
            ValidationException validationException = validate(value);
            if (validationException != null) return validationException;
            return resolver.apply((Integer) value, knnIndexContext);
        }

        @Override
        public ValidationException validate(Object value) {
            if (value != null && !(value instanceof Integer)) {
                ValidationException validationException = new ValidationException();
                validationException.addValidationError(
                    String.format(
                        "value is not an instance of MethodComponentContext for MethodComponentContext parameter [%s].",
                        getName()
                    )
                );
                throw validationException;
            }
            return validator.apply((Integer) value);
        }
    }

    /**
     * Double method parameter
     */
    public static class DoubleParameter extends Parameter<Double> {
        public DoubleParameter(
            String name,
            BiFunction<Double, KNNIndexContext, ValidationException> resolver,
            Function<Double, ValidationException> validator
        ) {
            super(name, resolver, validator);
        }

        @Override
        public ValidationException resolve(Object value, KNNIndexContext knnIndexContext) {
            ValidationException validationException = validate(value);
            if (validationException != null) return validationException;
            return resolver.apply((Double) value, knnIndexContext);
        }

        @Override
        public ValidationException validate(Object value) {
            if (value != null && value.equals(0)) value = 0.0;
            if (value != null && !(value instanceof Double)) {
                String validationErrorMsg = String.format(
                    Locale.ROOT,
                    "value is not an instance of Double for Double parameter [%s].",
                    getName()
                );
                return ValidationUtil.chainValidationErrors(null, validationErrorMsg);
            }
            return validator.apply((Double) value);
        }
    }

    /**
     * String method parameter
     */
    public static class StringParameter extends Parameter<String> {
        public StringParameter(
            String name,
            BiFunction<String, KNNIndexContext, ValidationException> resolver,
            Function<String, ValidationException> validator
        ) {
            super(name, resolver, validator);
        }

        @Override
        public ValidationException resolve(Object value, KNNIndexContext knnIndexContext) {
            ValidationException validationException = validate(value);
            if (validationException != null) return validationException;
            return resolver.apply((String) value, knnIndexContext);
        }

        @Override
        public ValidationException validate(Object value) {
            if (value != null && !(value instanceof String)) {
                ValidationException validationException = new ValidationException();
                validationException.addValidationError(
                    String.format("value is not an instance of String for String parameter [%s].", getName())
                );
                throw validationException;
            }
            return validator.apply((String) value);
        }
    }

    /**
     * MethodContext parameter. Some methods require sub-methods in order to implement some kind of functionality. For
     *  instance, faiss methods can contain an encoder along side the approximate nearest neighbor function to compress
     *  the input. This parameter makes it possible to add sub-methods to methods to support this kind of functionality
     */
    public static class MethodComponentContextParameter extends Parameter<MethodComponentContext> {

        private final Map<String, MethodComponent> methodComponent;

        public MethodComponentContextParameter(
            String name,
            BiFunction<MethodComponentContext, KNNIndexContext, ValidationException> resolver,
            Function<MethodComponentContext, ValidationException> validator,
            Map<String, MethodComponent> methodComponent
        ) {
            super(name, resolver, validator);
            this.methodComponent = methodComponent;
        }

        @Override
        public ValidationException resolve(Object value, KNNIndexContext knnIndexContext) {
            ValidationException validationException = validate(value);
            if (validationException != null) return validationException;
            return resolver.apply((MethodComponentContext) value, knnIndexContext);
        }

        @Override
        public ValidationException validate(Object value) {
            if (value != null && !(value instanceof MethodComponentContext)) {
                ValidationException validationException = new ValidationException();
                validationException.addValidationError(
                    String.format(
                        "value is not an instance of MethodComponentContext for MethodComponentContext parameter [%s].",
                        getName()
                    )
                );
                throw validationException;
            }
            return validator.apply((MethodComponentContext) value);
        }

        public MethodComponent getMethodComponent(String name) {
            return methodComponent.get(name);
        }
    }
}
