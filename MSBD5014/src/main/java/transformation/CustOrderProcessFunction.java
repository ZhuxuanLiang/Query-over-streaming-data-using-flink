package transformation;

import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import table.Orders;

import java.util.Date;
import java.util.Map;


public class CustOrderProcessFunction extends CoProcessFunction<Tuple2<Long,String>, Orders, Tuple3<Long, Date, String>>{
    private ValueState<Tuple2<Long,String>> nationCustState;
    private MapState<Long,Date> ordersMapState;
    @Override
    public void open(Configuration parameters) throws Exception {
        nationCustState = getRuntimeContext().getState(
                new ValueStateDescriptor<>
                        ("nationCust", Types.TUPLE(Types.LONG,Types.STRING)));
        ordersMapState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("order", Long.class, Date.class));
    }


    @Override
    public void processElement1(Tuple2<Long, String> nationCust, CoProcessFunction<Tuple2<Long, String>, Orders, Tuple3<Long, Date, String>>.Context context, Collector<Tuple3<Long, Date, String>> collector) throws Exception {
        if(nationCust.f1.equals("insert")){
            nationCustState.update(nationCust);
            for(Map.Entry<Long,Date> i: ordersMapState.entries()){
                collector.collect(Tuple3.of(i.getKey(),i.getValue(),nationCust.f1));
            }
        }else if(nationCust.f1.equals("delete")){
            if(nationCustState.value()!=null){
                for(Map.Entry<Long,Date> i: ordersMapState.entries()){
                    collector.collect(Tuple3.of(i.getKey(),i.getValue(),nationCust.f1));
                }
                nationCustState.clear();
            }
        }
    }

    @Override
    public void processElement2(Orders orders, CoProcessFunction<Tuple2<Long, String>, Orders, Tuple3<Long, Date, String>>.Context context, Collector<Tuple3<Long, Date, String>> collector) throws Exception {
        if(orders.operator.equals("insert")){
            ordersMapState.put(orders.o_orderkey,orders.o_orderdate);
            if(nationCustState.value()!=null){
                collector.collect(Tuple3.of(orders.o_orderkey,orders.o_orderdate,orders.operator));
            }
        }else if(orders.operator.equals("delete")){
            if(ordersMapState.contains(orders.o_orderkey)){
                ordersMapState.remove(orders.o_orderkey);
                if(nationCustState.value()!=null){
                    collector.collect(Tuple3.of(orders.o_orderkey,orders.o_orderdate,orders.operator));
                }
            }
        }
    }
}
