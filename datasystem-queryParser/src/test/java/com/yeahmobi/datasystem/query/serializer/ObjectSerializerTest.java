package com.yeahmobi.datasystem.query.serializer;


import org.junit.Test;

import com.yeahmobi.datasystem.query.config.Config;
import com.yeahmobi.datasystem.query.config.ConfigManager;
import com.yeahmobi.datasystem.query.meta.DimensionTable;
import com.yeahmobi.datasystem.query.meta.IntervalTable;
import com.yeahmobi.datasystem.query.meta.MetricAggTable;

public class ObjectSerializerTest {

    @Test
    public void testWriteStringT() {
        ConfigManager.getInstance().init();
        ObjectSerializer.write(Config.class.getSimpleName() + ".json", ConfigManager.getInstance().getCfg());

        ObjectSerializer.write(DimensionTable.class.getSimpleName() + ".json", new DimensionTable().getTable());

        ObjectSerializer.write(MetricAggTable.class.getSimpleName() + ".json", new MetricAggTable().getTable());

        ObjectSerializer.write(IntervalTable.class.getSimpleName() + ".json", IntervalTable.getTable());
    }

}
