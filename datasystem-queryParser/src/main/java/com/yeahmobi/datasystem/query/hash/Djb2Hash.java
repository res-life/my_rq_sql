package com.yeahmobi.datasystem.query.hash;
/**
 * Created by yangxu on 3/18/14.
 */

import org.apache.log4j.Logger;

public class Djb2Hash {

    private static Logger logger = Logger.getLogger(Djb2Hash.class);

    public static int hash(String str, int mask)
    {
        if (mask <= 0) return 0;
        long hash = 5381;
        for (byte c : str.getBytes()) {
            hash = ((hash << 5) + hash) + c; /* hash * 33 + c */
        }

        return (int)(hash & mask);
    }

}
