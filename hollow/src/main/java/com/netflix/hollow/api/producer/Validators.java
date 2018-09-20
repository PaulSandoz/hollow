package com.netflix.hollow.api.producer;

import com.netflix.hollow.api.producer.validation.AllValidationStatus;
import com.netflix.hollow.api.producer.validation.SingleValidationStatus;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EventListener;
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
         *
         * @@@ Skipping might be consired a sub-state of PASSED with details in ValidationResult
         */
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
     * A result of a {@link Validator}.
     * @@@ define builder
     */
    public static final class ValidationResult {
        private final ValidationResultType type;
        private final String name;
        private final Throwable ex; // set for status == ERROR or FAILED
        private final String message;
        private final Map<String, String> details;

        public ValidationResult(
                ValidationResultType type,
                String name,
                Throwable ex,
                String message,
                Map<String, String> details) {
            if (type == ValidationResultType.ERROR && ex == null) {
                throw new IllegalArgumentException();
            }
            // @@@ For the moment allow a throwable to be associated with FAILED state
            if (type == ValidationResultType.PASSED && ex != null) {
                throw new IllegalArgumentException();
            }

            this.name = Objects.requireNonNull(name);
            this.type = Objects.requireNonNull(type);
            this.ex = ex;
            this.message = message;
            this.details = Collections.unmodifiableMap(details);
        }

        public ValidationResultType getResultType() {
            return type;
        }

        public String getName() {
            return name;
        }

        public Throwable getThrowable() {
            return ex;
        }

        public String getMessage() {
            return message;
        }

        public Map<String, String> getDetails() {
            return details;
        }

        public boolean isPassed() {
            return type == ValidationResultType.PASSED;
        }
    }

    /**
     * A validator of {@link com.netflix.hollow.api.producer.HollowProducer.ReadState read state}.
     */
    // Any exception thrown will result in a ValidationResult instance with that exception
    // A validator should return a result with an error+exception if more information should be produced
    public interface Validator extends EventListener {
        /**
         * Gets the name of the validator.
         *
         * @return the name
         */
        String getName();

        /**
         * Called when validation is to be performed on read state.
         * <p>
         * If the a {@link RuntimeException} is thrown by the validator then validation is considered
         * to fail with the result of a validation {@Link ValidationResultType#ERROR error}.
         *
         * @param readState the read state.
         * @return a validation result
         */
        ValidationResult onValidate(HollowProducer.ReadState readState);
    }

    /**
     * The overall status of a sequence of validation results
     * (with the same order in which validators were executed).
     */
    public static final class ValidationStatus {
        private final boolean passed;
        private final List<ValidationResult> results;

        ValidationStatus(List<ValidationResult> results) {
            this.passed = results.stream().allMatch(ValidationResult::isPassed);
            this.results = Collections.unmodifiableList(results);
        }

        public boolean isPassed() {
            return passed;
        }

        public List<ValidationResult> getResults() {
            return results;
        }

        ValidationStatusException createValidationException() {
            return passed ? null : new ValidationStatusException(this);
        }
    }

    /**
     * A validation status exception holding a validation status.
     */
    public static final class ValidationStatusException extends RuntimeException {
        private final ValidationStatus status;

        ValidationStatusException(ValidationStatus status) {
            super("One or more validations failed. Please check individual failures.");

            this.status = Objects.requireNonNull(status);
            assert !status.isPassed();
        }

        public ValidationStatus getValidationStatus() {
            return status;
        }
    }

    /**
     * A listener of validation status start and complete events.
     */
    public interface ValidationStatusListener extends EventListener {
        void onValidationStatusStart(long version);

        void onValidationStatusComplete(ValidationStatus status, long version, long elapsed, TimeUnit unit);
    }


    //
    // Conversion between old and new validator API
    //

    static final class ValidatorProxy implements Validator {
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
            avsb.addSingelValidationStatus(b.build());
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
                exceptions.add(r.getThrowable());
            }
        }
        return new HollowProducer.Validator.ValidationException(
                "One or more validations failed. Please check individual failures.", exceptions);
    }
}