package transformation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.tuple.Tuple9;


public class PreForGroupBy implements MapFunction<Tuple8<Long, String, Double, Double, String, String, String, String>,
        Tuple9<String, Long, String, Double, Double, String, String, String, String>> {

    @Override
    public Tuple9<String, Long, String, Double, Double, String, String, String, String> map(Tuple8<Long, String, Double, Double, String, String, String, String> custLine) throws Exception {
        String temp = custLine.f0 + custLine.f1 + custLine.f3 + custLine.f4
                + custLine.f5 + custLine.f6 + custLine.f7;

        return Tuple9.of(temp, custLine.f0, custLine.f1, custLine.f2, custLine.f3, custLine.f4, custLine.f5, custLine.f6,custLine.f7);
    }
}
