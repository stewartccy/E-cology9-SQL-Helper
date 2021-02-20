package com.annotation;

import java.lang.annotation.*;

/**
 * author:liuguangdong
 * Date:2020/7/17 0017
 * Time:10:39
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface TableName {

    String value() default "";

}
