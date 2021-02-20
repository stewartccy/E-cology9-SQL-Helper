package com.annotation;

import java.lang.annotation.*;

/**
 * Date:2020/7/17 0018
 * Time:10:40
 */
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface ColumnName {

    String value() default "";

}
