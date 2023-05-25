package transformation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.Calendar;
import java.util.Date;

public class ExtractYear implements MapFunction<Tuple3<Long, Date,String>, Tuple3<Long,Integer,String>> {
    @Override
    public Tuple3<Long,Integer,String> map(Tuple3<Long,Date,String> extractyear) throws Exception {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(extractyear.f1);
        int year = calendar.get(Calendar.YEAR);

        return Tuple3.of(extractyear.f0,year,extractyear.f2);
    }
}
