package transformation;

import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import table.Nation;
import table.Region;

import java.util.Map;

public class RegionNationProcessFunction extends CoProcessFunction<Region, Nation, Tuple2<Long,String>> {
    private ValueState<Region> regionState;
    private MapState<Long,String> nationMapState;
    @Override
    public void open(Configuration parameters) throws Exception {
        regionState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("region", Types.POJO(Region.class)));
        nationMapState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("nation", Long.class,String.class));
    }
    @Override
    public void processElement1(Region region, CoProcessFunction<Region, Nation, Tuple2<Long,String>>.Context context, Collector<Tuple2<Long,String>> collector) throws Exception {
        if(region.operator.equals("insert")) {
            regionState.update(region);
            for (Map.Entry<Long, String> i : nationMapState.entries()) {
                collector.collect(Tuple2.of(i.getKey(), region.operator));
            }
        }else if(region.operator.equals("delete")){
            if(regionState.value()!=null){
                for(Map.Entry<Long,String>i: nationMapState.entries()){
                    collector.collect(Tuple2.of(i.getKey(),region.operator));
                }
                regionState.clear();
            }
        }
    }

    @Override
    public void processElement2(Nation nation, CoProcessFunction<Region, Nation, Tuple2<Long,String>>.Context context, Collector<Tuple2<Long,String>> collector) throws Exception {
        if(nation.operator.equals("insert")){
            nationMapState.put(nation.n_nationkey,nation.operator);
            if(regionState.value()!=null){
                collector.collect(Tuple2.of(nation.n_nationkey,nation.operator));
            }
        }else if(nation.operator.equals("delete")) {
            if (nationMapState.contains(nation.n_nationkey)) {
                nationMapState.remove(nation.n_nationkey);
                if(regionState.value()!=null){
                    collector.collect(Tuple2.of(nation.n_nationkey,nation.operator));
                }

            }
        }
    }
}
