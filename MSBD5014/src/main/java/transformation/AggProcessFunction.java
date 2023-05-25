package transformation;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import table.Lineitem;

public class AggProcessFunction extends KeyedProcessFunction<Integer, Tuple4<Integer,Double, String,String>, Tuple3<String,Integer,Double>> {
    AggregatingState<Tuple4<Integer, Double, String,String>, Tuple3<Double,Double,String>> aggState;
    MapState<Integer,Double> preState;

    public void open(Configuration parameters) throws Exception {
        aggState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Tuple4<Integer,Double,String,String>, Tuple3<Double,Double,String>, Tuple3<Double,Double,String>>(
                "sum",
                new AggregateFunction<Tuple4<Integer,Double,String,String>, Tuple3<Double,Double,String>, Tuple3<Double,Double,String>>() {

                    @Override
                    public Tuple3<Double, Double,String> createAccumulator() {
                        return Tuple3.of(0d,0d,"insert");
                    }

                    @Override
                    public Tuple3<Double, Double,String> add(Tuple4<Integer, Double, String,String> input, Tuple3<Double, Double,String> accumulator) {
                        Double tmp = 0d;
                        Double diff_1;
                        Double diff_2;
                        Double eps = 1e-10;
                        if (input.f2.equals("UNITED STATES")) {
                            tmp = input.f1;
                        }
                        if(input.f3.equals("insert")){
                            return Tuple3.of(accumulator.f0 + tmp, accumulator.f1+input.f1, input.f3);
                        }else {
                            diff_1 = accumulator.f1-input.f1;
                            diff_2 = accumulator.f0 - tmp;
                            if(diff_1<eps) diff_1=0d;
//                            if(diff_2<eps) diff_2=0d;
                            return Tuple3.of(diff_2, diff_1, input.f3);
                        }
                    }

                    @Override
                    public Tuple3<Double, Double, String> getResult(Tuple3<Double, Double,String> accumulator) {
                        return Tuple3.of(accumulator.f0/accumulator.f1, accumulator.f1, accumulator.f2);
                    }

                    @Override
                    public Tuple3<Double, Double,String> merge(Tuple3<Double, Double,String> doubleDoubleTuple2, Tuple3<Double, Double,String> acc1) {
                        return null;
                    }
                },
                Types.TUPLE(Types.DOUBLE,Types.DOUBLE,Types.STRING)
        ));
      preState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("preState", Integer.class,Double.class));
    }



    @Override
    public void processElement(Tuple4<Integer, Double, String, String> input, KeyedProcessFunction<Integer, Tuple4<Integer, Double, String, String>, Tuple3<String, Integer, Double>>.Context context, Collector<Tuple3<String, Integer, Double>> collector) throws Exception {
        if(input.f3.equals("insert")){
            aggState.add(input);
            if(!preState.contains(input.f0)){
                preState.put(input.f0, aggState.get().f0);
                collector.collect(Tuple3.of(input.f3, input.f0, aggState.get().f0));
            }else if( !preState.get(input.f0).equals(aggState.get().f0)) {
                preState.remove(input.f0);
                preState.put(input.f0, aggState.get().f0);
                collector.collect(Tuple3.of(input.f3, input.f0, aggState.get().f0));
            }
        }else{
            aggState.add(input);
            if(aggState.get().f1==0d){
                collector.collect(Tuple3.of(input.f3, input.f0, aggState.get().f0));
            }else{
                if(preState.contains(input.f0) && !preState.get(input.f0).equals(aggState.get().f0)){
                    preState.remove(input.f0);
                    preState.put(input.f0,aggState.get().f0);
                    collector.collect(Tuple3.of("update", input.f0, aggState.get().f0));
                }
            }
        }

    }
}
