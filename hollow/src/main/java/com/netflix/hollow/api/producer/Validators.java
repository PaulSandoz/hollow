package com.netflix.hollow.api.producer;

import com.netflix.hollow.api.producer.validation.AllValidationStatus;
import com.netflix.hollow.api.producer.validation.SingleValidationStatus;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Temporary enclosing class for the new validator API
 */
public class Validators {

    /**
     * The type of validation result.
     */
    public enum ValidationResultType {
        /**
         * The validation passed.
         */
        // @@@ Skipping might be considered a sub-state of PASSED with details in ValidationResult
        PASSED,
        /**
         * The validation failed.
         */
        FAILED,
        /**
         * The validator failed with an unexpected error and could not perform the validation
         */
        ERROR
    }

    /**
     * The result of validation performed by a {@link ValidatorListener validator}.
     */
    public static final class ValidationResult {
        private final ValidationResultType type;
        private final String name;
        private final Throwable ex; // set for status == ERROR or FAILED
        private final String message;
        private final Map<String, String> details;

        ValidationResult(
                ValidationResultType type,
                String name,
                Throwable ex,
                String message,
                Map<String, String> details) {
            if (type == ValidationResultType.ERROR && ex == null) {
                throw new IllegalArgumentException();
            }
            // @@@ For the moment allow a throwable to be associated with FAILED state
            // This is for compatibility with HollowProducer.Validator.ValidationException
            if (type == ValidationResultType.PASSED && ex != null) {
                throw new IllegalArgumentException();
            }

            assert name != null; // builder checks
            this.name = name;
            this.type = type;
            assert type != null; // builder ensures
            this.ex = ex;
            this.message = message;
            this.details = Collections.unmodifiableMap(details);
        }

        /**
         * Returns the validation result type.
         *
         * @return the validation result type
         */
        public ValidationResultType getResultType() {
            return type;
        }

        /**
         * Returns the name of the validation performed.
         *
         * @return the name of the validation performed
         */
        public String getName() {
            return name;
        }

        /**
         * Returns the {@code Throwable} associated with a validator that
         * failed with an unexpected {@link ValidationResultType#ERROR error}.
         *
         * @return the {@code Throwable} associated with an erroneous validator, otherwise
         * {@code null}
         */
        public Throwable getThrowable() {
            return ex;
        }

        /**
         * Returns a message associated with the validation.
         *
         * @return a message associated with the validation. may be {@code null}
         */
        public String getMessage() {
            return message;
        }

        /**
         * Returns details associated with the validation.
         *
         * @return details associated with the validation. The details are unmodifiable and may be empty.
         */
        public Map<String, String> getDetails() {
            return details;
        }

        /**
         * Returns true if validation passed, otherwise {@code false}.
         *
         * @return true if validation passed, otherwise {@code false}
         */
        public boolean isPassed() {
            return type == ValidationResultType.PASSED;
        }

        /**
         * Initiates the building of a result from a validation listener.
         * The {@link ValidationResult#getName name} of the validation result with be
         * set to the {@link ValidatorListener#getName name} of the validator
         *
         * @param v the validation listener
         * @return the validation builder
         * @throws NullPointerException if {@code v} is {@code null}
         */
        public static ValidationResultBuilder from(ValidatorListener v) {
            return from(v.getName());
        }

        /**
         * Initiates the building of a result from a name.
         *
         * @param name the validation result
         * @return the validation builder
         * @throws NullPointerException if {@code name} is {@code null}
         */
        public static ValidationResultBuilder from(String name) {
            return new ValidationResultBuilder(name);
        }

        /**
         * A builder of a {@link ValidationResult}.
         * <p>
         * The builder may be reused after it has built a validation result, but the details will be reset
         * to contain no entries.
         */
        static public class ValidationResultBuilder {
            private final String name;
            private Map<String, String> details;

            ValidationResultBuilder(String name) {
                this.name = Objects.requireNonNull(name);
                this.details = new HashMap<>();
            }

            /**
             * Sets a detail.
             *
             * @param name the detail name
             * @param value the detail value, which will be converted to a {@code String}
             * using {@link String#valueOf(Object)}
             * @return the validation builder
             * @throws NullPointerException if {@code name} or {@code value} are {@code null}
             */
            public ValidationResultBuilder detail(String name, Object value) {
                Objects.requireNonNull(name);
                Objects.requireNonNull(value);
                details.put(name, String.valueOf(value));
                return this;
            }

            /**
             * Builds a result that has {@link ValidationResultType#PASSED passed} with no message.
             *
             * @return a validation result that has passed.
             */
            public ValidationResult passed() {
                return new ValidationResult(
                        ValidationResultType.PASSED,
                        name,
                        null,
                        null,
                        details
                );
            }

            /**
             * Builds a result that has {@link ValidationResultType#PASSED passed} with a message.
             *
             * @param message the message, may be {@code null}
             * @return a validation result that has passed.
             */
            public ValidationResult passed(String message) {
                return build(
                        ValidationResultType.PASSED,
                        name,
                        null,
                        message,
                        details
                );
            }

            /**
             * Builds a result that has {@link ValidationResultType#FAILED failed} with a message.
             *
             * @param message the message
             * @return a validation result that has failed.
             * @throws NullPointerException if {@code message} is {@code null}
             */
            public ValidationResult failed(String message) {
                return build(
                        ValidationResultType.FAILED,
                        name,
                        null,
                        message,
                        details
                );
            }

            /**
             * Builds a result for a validator that produced an unexpected {@link ValidationResultType#ERROR error}
             * with a {@code Throwable}.
             *
             * @param t the {@code Throwable}
             * @return a validation result for a validator that produced an unexpected error
             * @throws NullPointerException if {@code t} is {@code null}
             */
            // @@@ This could be made package private if moved into producer package
            // as it is questionable if validators should use this, however for testing
            // purposes of status listeners its useful.
            public ValidationResult error(Throwable t) {
                return build(
                        ValidationResultType.FAILED,
                        name,
                        t,
                        t.getMessage(),
                        details
                );
            }

            private ValidationResult build(
                    ValidationResultType type,
                    String name,
                    Throwable ex,
                    String message,
                    Map<String, String> details) {
                reset();
                return new ValidationResult(type, name, ex, message, details);
            }

            private void reset() {
                this.details = new HashMap<>();
            }
        }
    }

    /**
     * A validator of {@link com.netflix.hollow.api.producer.HollowProducer.ReadState read state}.
     */
    public interface ValidatorListener extends HollowProducerListeners.HollowProducerEventListener {
        /**
         * Gets the name of the validator.
         *
         * @return the name
         */
        String getName();

        /**
         * Called when validation is to be performed on read state.
         * <p>
         * If a {@code RuntimeException} is thrown by the validator then validation is considered
         * to have failed with an unexpected error.  A {@link ValidationResult} will be built from this
         * validator as if by a call to {@link ValidationResult.ValidationResultBuilder#error(Throwable)}
         * with the {@code RuntimeException} as the argument.
         *
         * @param readState the read state.
         * @return the validation result
         */
        ValidationResult onValidate(HollowProducer.ReadState readState);

        // @@@ If we want to compose so a validator can aggregate multiple validators
        // then this default method could be added, which is then called during the validation status
        // cycle
//        default List<ValidationResult> onComposedValidate(HollowProducer.ReadState readState) {
//            return Collections.singletonList(onValidate(readState));
//        }
    }

    /**
     * The overall status of a sequence of validation results.
     * <p>
     * A status accumulated from the results of validators will be passed to
     * the {@link ValidationStatusListener validation status} listeners.
     * If one or more validation results have not passed then that status will also be reported
     * in a {@link ValidationStatusException} that is thrown and causes the current cycle to fail.
     */
    public static final class ValidationStatus {
        private final List<ValidationResult> results;
        private final boolean passed;

        /**
         * Creates a new validation status from a list of validation results.
         *
         * @param results the validation results
         * @throws NullPointerException if {@code results} is {@code null}
         */
        public ValidationStatus(List<ValidationResult> results) {
            this.results = Collections.unmodifiableList(new ArrayList<>(results));
            this.passed = this.results.stream().allMatch(ValidationResult::isPassed);
        }

        /**
         * Returns true if all validation results have passed, otherwise false if one or more results
         * failed or a validator was erroneous.
         *
         * @return true if all validation results have passed, otherwise false
         */
        public boolean isPassed() {
            return passed;
        }

        /**
         * Returns the validation results.
         *
         * @return the validation results. The results are unmodifiable.
         */
        public List<ValidationResult> getResults() {
            return results;
        }
    }

    /**
     * A validation status exception holding a validation status.
     */
    public static final class ValidationStatusException extends RuntimeException {
        private final ValidationStatus status;

        /**
         * Creates a validation status exception.
         *
         * @param status the status
         * @param message the message
         * @throws IllegalArgumentException if {@code status} contains results that all passed
         * @throws NullPointerException if {@code status} is {@code null}
         */
        public ValidationStatusException(ValidationStatus status, String message) {
            super(message);

            if (status.isPassed())
                throw new IllegalArgumentException("A validation status exception was created "
                        + "with a status containing results that all passed");

            this.status = Objects.requireNonNull(status);
        }

        /**
         * Returns the validation status.
         *
         * @return the validation status.
         */
        public ValidationStatus getValidationStatus() {
            return status;
        }
    }

    /**
     * A listener of validation status start and complete events.
     */
    public interface ValidationStatusListener extends HollowProducerListeners.HollowProducerEventListener {
        /**
         * Called before validation has started.
         *
         * @param version the version
         */
        void onValidationStatusStart(long version);

        /**
         * Called after validation has completed (all validation listeners have been called).
         *
         * @param status the validation status
         * @param version the version
         * @param elapsed the time elapsed between this event and {@link #onValidationStatusStart}
         * @param unit the time unit of elapsed time
         */
        void onValidationStatusComplete(ValidationStatus status, long version, long elapsed, TimeUnit unit);
    }


    //
    // Conversion between old and new validator API
    //

    static final class ValidatorProxy implements ValidatorListener {
        com.netflix.hollow.api.producer.HollowProducer.Validator hollowValidator;

        ValidatorProxy(com.netflix.hollow.api.producer.HollowProducer.Validator hollowValidator) {
            this.hollowValidator = hollowValidator;
        }

        @Override
        public String getName() {
            return (hollowValidator instanceof HollowProducer.Nameable)
                    ? ((HollowProducer.Nameable) hollowValidator).getName()
                    : "";
        }

        @Override
        public ValidationResult onValidate(HollowProducer.ReadState readState) {
            Throwable caught = null;
            try {
                hollowValidator.validate(readState);
            } catch (Throwable t) {
                // @@@ Catch throwable for compatibility
                caught = t;
            }
            return createValidationResult(hollowValidator, caught);
        }

        private static ValidationResult createValidationResult(HollowProducer.Validator validator, Throwable t) {
            String name = (validator instanceof HollowProducer.Nameable)
                    ? ((HollowProducer.Nameable) validator).getName()
                    : "";
            String message = validator.toString();

            ValidationResultType s = ValidationResultType.PASSED;
            if (t instanceof HollowProducer.Validator.ValidationException) {
                s = ValidationResultType.FAILED;
            } else if (t != null) {
                s = ValidationResultType.ERROR;
            }

            return new ValidationResult(s, name, t, message, Collections.emptyMap());
        }
    }

    static AllValidationStatus.AllValidationStatusBuilder createHollowAllValidationStatusBuilder(ValidationStatus s) {
        AllValidationStatus.AllValidationStatusBuilder avsb = AllValidationStatus.builder();
        if (s == null) {
            return avsb;
        }

        for (ValidationResult r : s.results) {
            SingleValidationStatus.SingleValidationStatusBuilder b = SingleValidationStatus.builder(r.name)
                    .withMessage(r.message);
            if (r.isPassed()) {
                b.success();
            } else {
                b.fail(r.ex);
            }
            avsb.addSingleValidationStatus(b.build());
        }

        if (s.isPassed()) {
            avsb.success();
        } else {
            avsb.fail();
        }
        return avsb;
    }

    static HollowProducer.Validator.ValidationException createHollowValidationException(ValidationStatusException e) {
        List<Throwable> exceptions = new ArrayList<>();
        for (Validators.ValidationResult r : e.getValidationStatus().getResults()) {
            if (!r.isPassed()) {
                Throwable t = r.getThrowable();
                if (t == null) {
                    t = new HollowProducer.Validator.ValidationException(r.getMessage());
                }
                exceptions.add(t);
            }
        }
        return new HollowProducer.Validator.ValidationException(
                "One or more validations failed. Please check individual failures.", exceptions);
    }
}