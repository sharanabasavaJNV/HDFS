import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable reducedIntegerWritable = new IntWritable(); 

   
    public void reduce(Text inputKey, Iterable<IntWritable> inputValues, Context context) throws IOException, InterruptedException {
        int wordCount = 0;
        
        for (IntWritable value : inputValues)
            wordCount += value.get();
        
        reducedIntegerWritable.set(wordCount);
        context.write(inputKey, reducedIntegerWritable);
    }
}
