package com.yeahmobi.datasystem.query.process;

import java.math.BigDecimal;

import com.yeahmobi.datasystem.query.meta.XCHANGE_RATE_BASE;

import static com.yeahmobi.datasystem.query.utils.MathUtils.*;

public enum Xchangers implements XchangerFunc {
    /* "cost","revenue","profit","epc","rpc","cpc","arpa","acpa" */
    cost{
        @Override
        public BigDecimal apply(Object ...args) {
            XCHANGE_RATE_BASE xchange_rate_base = (XCHANGE_RATE_BASE)args[0];
            Object val = args[1];
            String valueStr = String.valueOf(val);
            return numberFormat3(Double.valueOf(valueStr) * xchange_rate_base.getRate_usd_to(), getPrecisionDigits(valueStr));
        }
    },
    revenue{
        @Override
        public BigDecimal apply(Object ...args) {
            XCHANGE_RATE_BASE xchange_rate_base = (XCHANGE_RATE_BASE)args[0];
            Object val = args[1];
            String valueStr = String.valueOf(val);
            return numberFormat3(Double.valueOf(valueStr) * xchange_rate_base.getRate_from_to(), getPrecisionDigits(valueStr));
        }
    },
    profit{
        @Override
        public BigDecimal apply(Object ...args) {
            XCHANGE_RATE_BASE xchange_rate_base = (XCHANGE_RATE_BASE)args[0];
            String revenueVal = String.valueOf(args[1]);
            String costVal = String.valueOf(args[2]);
            String profitVal = String.valueOf(args[3]);
            int precisionDigit = getPrecisionDigits(profitVal);
            return numberFormat3(numberFormat3((Double.valueOf(revenueVal) * xchange_rate_base.getRate_from_to()), precisionDigit).subtract(numberFormat3((Double.valueOf(costVal) * xchange_rate_base.getRate_usd_to()), precisionDigit)), precisionDigit);
        }
    },
    epc{
        @Override
        public BigDecimal apply(Object ...args) {
            XCHANGE_RATE_BASE xchange_rate_base = (XCHANGE_RATE_BASE)args[0];
            String val = String.valueOf(args[1]);
            
            return numberFormat3(Double.valueOf(val) * xchange_rate_base.getRate_usd_to(), getPrecisionDigits(val));
        }
    },
    rpc{
        @Override
        public BigDecimal apply(Object ...args) {
            XCHANGE_RATE_BASE xchange_rate_base = (XCHANGE_RATE_BASE)args[0];
            String val = String.valueOf(args[1]);
            return numberFormat3(Double.valueOf(val) * xchange_rate_base.getRate_from_to(), getPrecisionDigits(val));
        }
    },
    cpc{
        @Override
        public BigDecimal apply(Object ...args) {
            XCHANGE_RATE_BASE xchange_rate_base = (XCHANGE_RATE_BASE)args[0];
            String val = String.valueOf(args[1]);
            return numberFormat3(Double.valueOf(val) * xchange_rate_base.getRate_usd_to(), getPrecisionDigits(val));
        }
    },
    arpa{
        @Override
        public BigDecimal apply(Object ...args) {
            XCHANGE_RATE_BASE xchange_rate_base = (XCHANGE_RATE_BASE)args[0];
            String val = String.valueOf(args[1]);
            return numberFormat3(Double.valueOf(val) * xchange_rate_base.getRate_from_to(), getPrecisionDigits(val));
        }
    },
    acpa{
        @Override
        public BigDecimal apply(Object ...args) {
            XCHANGE_RATE_BASE xchange_rate_base = (XCHANGE_RATE_BASE)args[0];
            String val = String.valueOf(args[1]);
            return numberFormat3(Double.valueOf(val) * xchange_rate_base.getRate_usd_to(), getPrecisionDigits(val));
        }
    };
}
