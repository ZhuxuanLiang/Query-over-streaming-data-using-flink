package transformation;

import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import table.Lineitem;
import table.Part;

import java.util.Map;

public class ConnectPartProcessFunction extends CoProcessFunction<Tuple5<String,Integer,Double,Long,String>, Part, Tuple4<String,Integer,Double,String>> {
    private MapState<String, Tuple5<String,Integer,Double,Long,String>> connectPartState;
    private ValueState<Part> partState;

    @Override
    public void open(Configuration parameters) throws Exception {
       connectPartState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>
                        ("connectpart", Types.STRING,Types.TUPLE(Types.STRING,Types.INT,Types.DOUBLE, Types.LONG)));
       partState = getRuntimeContext().getState(
                new ValueStateDescriptor<Part>("part", Types.POJO(Part.class)));
    }
    @Override
    public void processElement1(Tuple5<String, Integer, Double, Long,String> connectpart, CoProcessFunction<Tuple5<String, Integer, Double, Long,String>, Part,
            Tuple4<String, Integer, Double,String>>.Context context, Collector<Tuple4<String, Integer, Double,String>> collector) throws Exception {
        if (connectpart.f4.equals("insert")) {
            connectPartState.put(connectpart.f0, connectpart);
            if (partState.value() != null) {
                collector.collect(Tuple4.of(connectpart.f0, connectpart.f1, connectpart.f2, connectpart.f4));
            }
        } else if (connectpart.f4.equals("delete")) {
            if (connectPartState.contains(connectpart.f0)) {
                connectPartState.remove(connectpart.f0);
                if (partState.value() != null) {
                    collector.collect(Tuple4.of(connectpart.f0, connectpart.f1, connectpart.f2, connectpart.f4));
                }
            }
        }
    }
    @Override
    public void processElement2(Part part, CoProcessFunction<Tuple5<String, Integer, Double,Long,String>, Part, Tuple4<String, Integer, Double,String>>.Context context,
                                Collector<Tuple4<String, Integer, Double,String>> collector) throws Exception {
        if(part.operator.equals("insert")){
            partState.update(part);
            for(Map.Entry<String, Tuple5<String,Integer,Double,Long,String>> i: connectPartState.entries()){
                collector.collect(Tuple4.of(i.getValue().f0,i.getValue().f1,i.getValue().f2,part.operator));
            }
        }else if(part.operator.equals("delete")){
            if(partState.value()!=null){
                for(Map.Entry<String, Tuple5<String,Integer,Double,Long,String>> i: connectPartState.entries()){
                    collector.collect(Tuple4.of(i.getValue().f0,i.getValue().f1,i.getValue().f2,part.operator));
                }
                partState.clear();
            }
        }
    }
}
