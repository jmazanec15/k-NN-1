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
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * Parameter that can be set for a method component
 *
 * @param <T> Type parameter takes
 */
public abstract class Parameter<T> {
    @Getter
    private final String name;
    protected final BiConsumer<T, KNNLibraryIndex.Builder> resolver;
    protected final Function<T, ValidationException> validator;

    /**
     * Constructor
     *
     * @param name of the parameter
     * @param resolver resolves the parameter
     */
    public Parameter(String name, BiConsumer<T, KNNLibraryIndex.Builder> resolver, Function<T, ValidationException> validator) {
        this.name = name;
        this.resolver = resolver;
        this.validator = validator;
    }

    /**
     * Resolve the provided parameters for the given configuration
     *
     * @param value to be checked
     */
    public void resolve(Object value, KNNLibraryIndex.Builder builder) {
        ValidationException validationException = validate(value);
        if (validationException != null) {
            builder.addValidationErrorMessage(validationException.getMessage());
            return;
        }
        resolver.accept(doCast(value), builder);
    }

    /**
     * Validate that an object is a valid parameter
     *
     * @param value {@link Object}
     * @return {@link ValidationException} or null if valid
     */
    public abstract ValidationException validate(Object value);

    protected abstract T doCast(Object value);

    /**
     * Boolean method parameter
     */
    public static class BooleanParameter extends Parameter<Boolean> {
        public BooleanParameter(
            String name,
            BiConsumer<Boolean, KNNLibraryIndex.Builder> resolver,
            Function<Boolean, ValidationException> validator
        ) {
            super(name, resolver, validator);
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

        @Override
        protected Boolean doCast(Object value) {
            return (Boolean) value;
        }
    }

    /**
     * Integer method parameter
     */
    public static class IntegerParameter extends Parameter<Integer> {
        public IntegerParameter(
            String name,
            BiConsumer<Integer, KNNLibraryIndex.Builder> resolver,
            Function<Integer, ValidationException> validator
        ) {
            super(name, resolver, validator);
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

        @Override
        protected Integer doCast(Object value) {
            return (Integer) value;
        }
    }

    /**
     * Double method parameter
     */
    public static class DoubleParameter extends Parameter<Double> {
        public DoubleParameter(
            String name,
            BiConsumer<Double, KNNLibraryIndex.Builder> resolver,
            Function<Double, ValidationException> validator
        ) {
            super(name, resolver, validator);
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

        @Override
        protected Double doCast(Object value) {
            return (Double) value;
        }
    }

    /**
     * String method parameter
     */
    public static class StringParameter extends Parameter<String> {
        public StringParameter(
            String name,
            BiConsumer<String, KNNLibraryIndex.Builder> resolver,
            Function<String, ValidationException> validator
        ) {
            super(name, resolver, validator);
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

        @Override
        protected String doCast(Object value) {
            return (String) value;
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
            BiConsumer<MethodComponentContext, KNNLibraryIndex.Builder> resolver,
            Function<MethodComponentContext, ValidationException> validator,
            Map<String, MethodComponent> methodComponent
        ) {
            super(name, resolver, validator);
            this.methodComponent = methodComponent;
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

        @Override
        protected MethodComponentContext doCast(Object value) {
            return (MethodComponentContext) value;
        }
    }
}
