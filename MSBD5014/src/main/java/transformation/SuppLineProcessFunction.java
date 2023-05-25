package transformation;


import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import table.Lineitem;
import table.Supplier;

import java.util.Map;


public class SuppLineProcessFunction extends CoProcessFunction<Tuple3<String,Long,String>, Lineitem,Tuple3<String,String,String>> {
    private ValueState<Tuple3<String, Long, String>> nationSuppState;
    private MapState<String, String> lineMapState;

    @Override
    public void open(Configuration parameters) throws Exception {
        nationSuppState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("nationsupp", Types.TUPLE(Types.STRING, Types.LONG, Types.STRING)));
        lineMapState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("lineitem", String.class, String.class));

    }

    @Override
    public void processElement1(Tuple3<String, Long, String> nationsupp, CoProcessFunction<Tuple3<String, Long, String>, Lineitem, Tuple3<String, String, String>>.Context context, Collector<Tuple3<String, String, String>> collector) throws Exception {
        if (nationsupp.f2.equals("insert")) {
            nationSuppState.update(nationsupp);
            for (Map.Entry<String, String> i : lineMapState.entries()) {

                collector.collect(Tuple3.of(nationsupp.f0, i.getKey(), nationsupp.f2));
            }
        } else if (nationsupp.f2.equals("delete")) {
            if (nationSuppState.value() != null) {
                for (Map.Entry<String, String> i : lineMapState.entries()) {
                    collector.collect(Tuple3.of(nationsupp.f0, i.getKey(), nationsupp.f2));
                }
                nationSuppState.clear();
            }
        }

    }


    @Override
    public void processElement2(Lineitem lineitem, CoProcessFunction<Tuple3<String, Long, String>, Lineitem, Tuple3<String, String, String>>.Context context, Collector<Tuple3<String, String, String>> collector) throws Exception {
        String temp = Long.toString(lineitem.l_orderkey) + lineitem.l_linenumber;
        if (lineitem.operator.equals("insert")) {
            lineMapState.put(temp, lineitem.operator);
            if (nationSuppState.value() != null) {
                collector.collect(Tuple3.of(nationSuppState.value().f0, temp, lineitem.operator));
            }
        } else if (lineitem.operator.equals("delete")) {
            if (lineMapState.contains(temp)) {
                lineMapState.remove(temp);
                if (nationSuppState.value() != null) {
                    collector.collect(Tuple3.of(nationSuppState.value().f0, temp, lineitem.operator));
                }
            }
        }
    }
}
