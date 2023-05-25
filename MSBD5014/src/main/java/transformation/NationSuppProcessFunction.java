package transformation;

import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import table.Nation;
import table.Supplier;
import java.util.Map;

public class NationSuppProcessFunction extends CoProcessFunction<Nation, Supplier, Tuple3<String,Long,String>> {
    private ValueState<Nation> nationState;
    private MapState<Long, String> suppMapState;

    @Override
    public void open(Configuration parameters) throws Exception {
        nationState = getRuntimeContext().getState(
                new ValueStateDescriptor<Nation>("nation", Types.POJO(Nation.class)));
        suppMapState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("supplier", Long.class, String.class));
    }
    @Override
    public void processElement1(Nation nation, CoProcessFunction<Nation, Supplier, Tuple3<String, Long, String>>.Context context, Collector<Tuple3<String, Long, String>> collector) throws Exception {
        if(nation.operator.equals("insert")) {
            nationState.update(nation);
            for(Map.Entry<Long,String> i : suppMapState.entries()){
                collector.collect(Tuple3.of(nation.n_name, i.getKey(),nation.operator));
            }
        }else if(nation.operator.equals("delete")) {
            if (nationState.value() != null) {
                for (Map.Entry<Long,String> i : suppMapState.entries()) {
                    collector.collect(Tuple3.of(nation.n_name, i.getKey(), nation.operator));
                }
                nationState.clear();
            }
        }

    }

    @Override
    public void processElement2(Supplier supplier, CoProcessFunction<Nation, Supplier, Tuple3<String, Long, String>>.Context context, Collector<Tuple3<String, Long, String>> collector) throws Exception {
        if(supplier.operator.equals("insert")){
            suppMapState.put(supplier.s_suppkey, supplier.operator);
            if(nationState.value()!=null){
                collector.collect(Tuple3.of(nationState.value().n_name, supplier.s_suppkey, supplier.operator));
            }

        }else if(supplier.operator.equals("delete")){
            if(suppMapState.contains(supplier.s_suppkey)){
                suppMapState.remove(supplier.s_suppkey);
                if(nationState.value()!=null){
                    collector.collect(Tuple3.of(nationState.value().n_name, supplier.s_suppkey, supplier.operator));
                }
            }

        }
    }
}


