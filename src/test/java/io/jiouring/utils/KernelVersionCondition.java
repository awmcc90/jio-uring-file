package io.jiouring.utils;

import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.commons.util.AnnotationUtils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class KernelVersionCondition implements ExecutionCondition {

    private static final Pattern VERSION_PATTERN = Pattern.compile("^(\\d+)\\.(\\d+)");

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
        var optional = AnnotationUtils.findAnnotation(context.getElement(), RequiresKernel.class);
        if (optional.isEmpty()) {
            return ConditionEvaluationResult.enabled("No @RequiresKernel annotation present");
        }

        String requiredVersion = optional.get().value();
        String currentOsVersion = System.getProperty("os.version");

        double required = parseVersion(requiredVersion);
        double current = parseVersion(currentOsVersion);

        if (current >= required) {
            return ConditionEvaluationResult.enabled(
                String.format("Kernel %s is >= %s", currentOsVersion, requiredVersion));
        } else {
            return ConditionEvaluationResult.disabled(
                String.format("Kernel %s is older than required %s", currentOsVersion, requiredVersion));
        }
    }

    private double parseVersion(String version) {
        try {
            Matcher matcher = VERSION_PATTERN.matcher(version);
            if (matcher.find()) {
                String major = matcher.group(1);
                String minor = matcher.group(2);
                return Double.parseDouble(major + "." + minor);
            }
        } catch (Exception ignored) {
        }
        return 0.0;
    }
}
