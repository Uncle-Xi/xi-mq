package com.ximq.common.annotation;

import java.lang.annotation.*;

@Target({ ElementType.TYPE, ElementType.METHOD, ElementType.ANNOTATION_TYPE })
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface XiMQListener {

    String groupId() default "";

    String[] topics() default {};
}
