package com.yeahmobi.datasystem.query.akka.cache;

import com.google.common.base.Strings;
import com.google.common.hash.Hashing;
import com.yeahmobi.datasystem.query.hash.MurmurHash;

import java.nio.charset.Charset;

/**
 * Created by yangxu on 5/9/14.
 */

public enum UniqHashFunction implements HashFunction {
    Md5 {
        @Override
        public String apply(String input) {
            return Hashing.md5().hashString(Strings.nullToEmpty(input), Charset.forName("UTF-8")).toString();
        }
    },
    Murmur {
        @Override
        public String apply(String input) {
            return "" + MurmurHash.hash64(Strings.nullToEmpty(input));
        }
    },
    Sha1 {
        @Override
        public String apply(String input) {
            return Hashing.sha1().hashString(Strings.nullToEmpty(input), Charset.forName("UTF-8")).toString();
        }
    },
    Plain {
        @Override
        public String apply(String input) {
            return Strings.nullToEmpty(input);
        }
    }

}
