package transformation;

import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import table.Lineitem;

import java.util.Map;


public class OrderLineProcessFunction extends CoProcessFunction<Tuple3<Long, Integer,String>, Lineitem, Tuple5<String,Integer,Double,Long,String>> {
    private ValueState<Tuple3<Long,Integer,String>> custOrderState;
    private MapState<String,Lineitem> lineMapState;
    @Override
    public void open(Configuration parameters) throws Exception {
        custOrderState = getRuntimeContext().getState(
                new ValueStateDescriptor<>
                        ("nationCust", Types.TUPLE(Types.LONG,Types.INT,Types.STRING)));
        lineMapState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("lineitem", BasicTypeInfo.STRING_TYPE_INFO,Types.POJO(Lineitem.class)));
    }
    @Override
    public void processElement1(Tuple3<Long,Integer,String> CustOrder,
                                CoProcessFunction<Tuple3<Long, Integer,String>, Lineitem, Tuple5<String, Integer,Double, Long,String>>.Context context,
                                Collector<Tuple5<String, Integer, Double,Long,String>> collector) throws Exception {
        if(CustOrder.f2.equals("insert")){
            custOrderState.update(CustOrder);
            for(Map.Entry<String,Lineitem> i: lineMapState.entries()){
                Double volume = i.getValue().l_extendedprice * (1-i.getValue().l_discount);
                collector.collect(Tuple5.of(i.getKey(),custOrderState.value().f1,volume,i.getValue().l_partkey,CustOrder.f2));
            }
        }else if(CustOrder.f2.equals("delete")){
            if(custOrderState.value()!=null){
                for(Map.Entry<String,Lineitem> i: lineMapState.entries()){
                    Double volume = i.getValue().l_extendedprice * (1-i.getValue().l_discount);
                    collector.collect(Tuple5.of(i.getKey(),custOrderState.value().f1,volume,i.getValue().l_partkey,CustOrder.f2));
                }
                custOrderState.clear();
            }
        }


    }

    @Override
    public void processElement2(Lineitem lineitem, CoProcessFunction<Tuple3<Long,Integer,String>, Lineitem, Tuple5<String,Integer,Double,Long,String>>.Context context, Collector<Tuple5<String,Integer,Double,Long,String>> collector) throws Exception {
        String temp = Long.toString(lineitem.l_orderkey) + lineitem.l_linenumber;
        if(lineitem.operator.equals("insert")){
            lineMapState.put(temp,lineitem);
            if(custOrderState.value()!=null){
                Double volume = lineitem.l_extendedprice * (1-lineitem.l_discount);
                collector.collect(Tuple5.of(temp,custOrderState.value().f1,volume,lineitem.l_partkey,lineitem.operator));
            }
        }else if(lineitem.operator.equals("delete")){
            if(lineMapState.contains(temp)){
                lineMapState.remove(temp);
                if(custOrderState.value()!=null){
                    Double volume = lineitem.l_extendedprice * (1-lineitem.l_discount);
                    collector.collect(Tuple5.of(temp,custOrderState.value().f1,volume,lineitem.l_partkey,lineitem.operator));
                }
            }
        }
    }
}
