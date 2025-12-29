package io.jiouring.utils;

import org.junit.jupiter.api.extension.ExtendWith;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.annotation.ElementType;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.TYPE})
@ExtendWith(KernelVersionCondition.class)
public @interface RequiresKernel {
    String value();
}
