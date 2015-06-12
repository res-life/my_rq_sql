package com.yeahmobi.datasystem.query.cache; 

import com.yeahmobi.datasystem.query.hash.Djb2Hash;
import org.junit.Assert;
import org.junit.Test;
import org.junit.Before; 
import org.junit.After; 

/** 
* Djb2Hash Tester.
* 
* @author <YangXu> 
* @since <pre>Mar 25, 2014</pre> 
* @version 1.0 
*/ 
public class HashTest { 

@Before
public void before() throws Exception { 
} 

@After
public void after() throws Exception { 
} 

/** 
* 
* Method: djb2(String str, int mask) 
* 
*/ 
@Test
public void testDjb2() throws Exception { 
//TODO: Test goes here...
    for (int i = 0; i < 1000; ++i)
    Assert.assertEquals(2, Djb2Hash.hash("69b1daea-5b77-4e80-846f-ce19cbc19e4e", 3));
} 


} 
