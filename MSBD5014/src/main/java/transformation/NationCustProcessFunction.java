package transformation;

import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import table.Customer;
import table.Nation;

import java.util.Map;

public class NationCustProcessFunction extends CoProcessFunction<Tuple2<Long,String>, Customer, Tuple2<Long,String>> {
    private ValueState<Tuple2<Long,String>> nationState;
    private MapState<Long,String> custMapState;

    @Override
    public void open(Configuration parameters) throws Exception {
        nationState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("nation", Types.TUPLE(Types.LONG,Types.STRING)));
        custMapState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("customer",Long.class,String.class));
    }

    @Override
    public void processElement1(Tuple2<Long, String> nation, CoProcessFunction<Tuple2<Long, String>, Customer, Tuple2<Long, String>>.Context context, Collector<Tuple2<Long, String>> collector) throws Exception {
        if(nation.f1.equals("insert")){
            nationState.update(nation);
            for(Map.Entry<Long,String> i: custMapState.entries()){
                collector.collect(Tuple2.of(i.getKey(),nation.f1));
            }
        }else if(nation.f1.equals("delete")){
            if(nationState.value()!=null){
                for(Map.Entry<Long,String> i: custMapState.entries()){
                    collector.collect(Tuple2.of(i.getKey(),nation.f1));
                }
                nationState.clear();
            }
        }
    }

    @Override
    public void processElement2(Customer customer, CoProcessFunction<Tuple2<Long, String>, Customer, Tuple2<Long, String>>.Context context, Collector<Tuple2<Long, String>> collector) throws Exception {
        if(customer.operator.equals("insert")){
            custMapState.put(customer.c_custkey, customer.operator);
            if(nationState.value()!=null){
                collector.collect(Tuple2.of(customer.c_custkey,customer.operator));
            }
        }else if(customer.operator.equals("delete")){
            if(custMapState.contains(customer.c_custkey)){
                custMapState.remove(customer.c_custkey);
                if(nationState.value()!=null){
                    collector.collect(Tuple2.of(customer.c_custkey,customer.operator));
                }

            }
        }
    }
}

