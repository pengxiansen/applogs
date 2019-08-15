package com.pengzhaopeng.analysis.registrator;

import com.esotericsoftware.kryo.Kryo;
import com.pengzhaopeng.comon.StartupReportLogs;
import org.apache.spark.serializer.KryoRegistrator;

public class MyKryoRegistrator implements KryoRegistrator {
    @Override
    public void registerClasses(Kryo kryo) {
        kryo.register(StartupReportLogs.class);
    }
}
