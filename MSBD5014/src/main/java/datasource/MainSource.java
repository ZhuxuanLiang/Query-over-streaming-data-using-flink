package datasource;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.io.File;
import java.util.Scanner;

public class MainSource implements ParallelSourceFunction<String> {
    private Boolean running = true;
    public String filePath;
    public MainSource(String filePath) {
        this.filePath = filePath;
    }
    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        Scanner sc = new Scanner(new File(filePath));
        while(running){
            String data= sc.nextLine();
            sourceContext.collect(data);
            if (!sc.hasNext()) {
                running = false;
            }
        }
    }

    @Override
    public void cancel() {
        running = false;

    }
}

