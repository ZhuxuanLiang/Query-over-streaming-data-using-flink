package transformation;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;


public class ConnectTwoStream extends CoProcessFunction<Tuple4<String,Integer,Double,String>, Tuple3<String,String,String>,Tuple4<Integer,Double,String,String>>{
    private ValueState<Tuple4<String,Integer,Double,String>> mat1ListState;
    private ValueState<Tuple3<String,String,String>> mat2ListState;
    @Override
    public void open(Configuration parameters) throws Exception {
       mat1ListState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("mat1", Types.TUPLE(Types.STRING,Types.INT,Types.DOUBLE,Types.STRING)));
       mat2ListState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("mat2", Types.TUPLE(Types.STRING,Types.STRING,Types.STRING)));
    }
    @Override
    public void processElement1(Tuple4<String, Integer, Double,String> mat1, CoProcessFunction<Tuple4<String, Integer, Double,String>, Tuple3<String, String,String>, Tuple4<Integer, Double, String,String>>.Context context, Collector<Tuple4<Integer, Double, String,String>> collector) throws Exception {
        if(mat1.f3.equals("insert")){
            mat1ListState.update(mat1);
            if(mat2ListState.value()!=null){
                collector.collect(Tuple4.of(mat1.f1, mat1.f2, mat2ListState.value().f0,mat1.f3));
            }
        }else if(mat1.f3.equals("delete")){
          if(mat1ListState.value()!=null){
              if(mat2ListState.value()!=null){
                  collector.collect(Tuple4.of(mat1.f1, mat1.f2, mat2ListState.value().f0,mat1.f3));
              }
              mat1ListState.clear();
          }
        }
    }

    @Override
    public void processElement2(Tuple3<String, String, String> mat2, CoProcessFunction<Tuple4<String, Integer, Double, String>, Tuple3<String, String, String>, Tuple4<Integer, Double, String, String>>.Context context, Collector<Tuple4<Integer, Double, String, String>> collector) throws Exception {
        if(mat2.f2.equals("insert")){
            mat2ListState.update(mat2);
            if(mat1ListState.value()!=null){
                collector.collect(Tuple4.of(mat1ListState.value().f1,  mat1ListState.value().f2, mat2.f0, mat2.f2));
            }
        }else if(mat2.f2.equals("delete")){
            if(mat2ListState.value()!=null){
                if(mat1ListState.value()!=null){
                    collector.collect(Tuple4.of(mat1ListState.value().f1,  mat1ListState.value().f2, mat2.f0, mat2.f2));
                }
                mat2ListState.clear();
            }
        }
    }


}
