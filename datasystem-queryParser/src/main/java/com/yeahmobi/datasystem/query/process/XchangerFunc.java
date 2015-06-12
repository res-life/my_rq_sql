package com.yeahmobi.datasystem.query.process;

import com.yeahmobi.datasystem.query.meta.XCHANGE_RATE_BASE;

public interface XchangerFunc {
    // public Object apply(Object val, XCHANGE_RATE_BASE xchange_rate_base);
    public Object apply(Object ...args);
}
