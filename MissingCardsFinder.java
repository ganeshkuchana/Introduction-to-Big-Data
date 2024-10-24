import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MissingCardsFinder {
    
    public static class CardMapper extends Mapper<Object, Text, Card, NullWritable> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] cardInfo = value.toString().trim().split("\\s+");
            if (cardInfo.length == 2) {
                context.write(new Card(cardInfo[1], cardInfo[0]), NullWritable.get());
            }
        }
    }
    
    public static class CardReducer extends Reducer<Card, NullWritable, Text, NullWritable> {
        private static final String[] SUITS = {"Hearts", "Diamonds", "Clubs", "Spades"};
        private static final String[] RANKS = {"2", "3", "4", "5", "6", "7", "8", "9", "10", "Jack", "Queen", "King", "Ace"};
        
        public void reduce(Card key, Iterable<NullWritable> values, Context context) 
                throws IOException, InterruptedException {
            Set<String> presentCards = new HashSet<>();
            presentCards.add(key.toString());
            
            // Generate all possible cards and find missing ones
            for (String suit : SUITS) {
                for (String rank : RANKS) {
                    String card = rank + " of " + suit;
                    if (!presentCards.contains(card)) {
                        context.write(new Text(card), NullWritable.get());
                    }
                }
            }
        }
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "missing cards finder");
        
        job.setJarByClass(MissingCardsFinder.class);
        job.setMapperClass(CardMapper.class);
        job.setReducerClass(CardReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setMapOutputKeyClass(Card.class);
        job.setMapOutputValueClass(NullWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
