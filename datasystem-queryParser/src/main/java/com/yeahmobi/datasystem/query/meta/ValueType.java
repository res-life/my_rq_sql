package com.yeahmobi.datasystem.query.meta;

import io.druid.query.dimension.DimensionType;

/**
 * 值类型
 * Created by yangxu on 3/18/14.
 */
public enum ValueType {
    NUMBER {
        @Override
        public DimensionType toDimType() {
            return DimensionType.DECIMAL;
        }
    },

    DECIMAL {
        @Override
        public DimensionType toDimType() {
            return DimensionType.DECIMAL;
        }
    },

    INTEGER {
        @Override
        public DimensionType toDimType() {
            return DimensionType.INTEGER;
        }
    },

    STRING {
        @Override
        public DimensionType toDimType() {
            return DimensionType.PLAIN;
        }
    },

    UNKNOWN {
        @Override
        public DimensionType toDimType() {
            return DimensionType.PLAIN;
        }
    };

    public abstract DimensionType toDimType();
}
