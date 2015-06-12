package com.yeahmobi.datasystem.query.akka.cache;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** 
* UniqHashFunction Tester.
* 
* @author <YangXu> 
* @since <pre>May 9, 2014</pre> 
* @version 1.0 
*/ 
public class UniqueKeyFuncTest {

    @Before
    public void before() throws Exception {
    }

    @After
    public void after() throws Exception {
    }

    @Test
    public void test() throws Exception {
        System.out.println(UniqHashFunction.Md5.apply("abc"));
        System.out.println(UniqHashFunction.Sha1.apply("abc"));
        System.out.println(UniqHashFunction.Murmur.apply("abc"));
        System.out.println(UniqHashFunction.Md5.apply("abc"));
    }

} 
