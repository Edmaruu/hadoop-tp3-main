package org.epf.hadoop.colfil1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RelationshipReducer extends Reducer<Text, Text, Text, Text> {
    private Text friendsList = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<String> friends = new ArrayList<>();
        for (Text value : values) {
            friends.add(value.toString());
        }
        friendsList.set(String.join(",", friends));
        context.write(key, friendsList);
    }
}
