package com.yeahmobi.datasystem.query.akka.cache; 

import org.junit.Test; 
import org.junit.Before; 
import org.junit.After; 

/** 
* RedisQueryCache Tester. 
* 
* @author <YangXu> 
* @since <pre>May 9, 2014</pre> 
* @version 1.0 
*/ 
public class RedisQueryCacheTest { 

@Before
public void before() throws Exception { 
} 

@After
public void after() throws Exception { 
} 

/** 
* 
* Method: getInstance() 
* 
*/ 
@Test
public void testGetInstance() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: set(K key, V object) 
* 
*/ 
@Test
public void testSet() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: get(K key) 
* 
*/ 
@Test
public void testGetKey() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: get(K key, Class<V> clazz) 
* 
*/ 
@Test
public void testGetForKeyClazz() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: cacheKey(K input) 
* 
*/ 
@Test
public void testCacheKeyInput() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: cacheKey(Object... inputs) 
* 
*/ 
@Test
public void testCacheKeyInputs() throws Exception {
    QueryCache cache = QueryCacheFactory.create("redis");
    System.out.println(cache.cacheKey("abc", 1));
} 


} 
