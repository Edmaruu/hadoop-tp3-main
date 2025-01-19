package org.epf.hadoop.colfil2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class CommonFriendReducer extends Reducer<UserPair, Text, UserPair, Text> {
    private final Text result = new Text();

    protected void reduce(UserPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Use a set to identify common relations
        HashSet<String> relations = new HashSet<>();
        int commonCount = 0;

        for (Text value : values) {
            if (!relations.add(value.toString())) {
                // If value already exists in the set, it's a common relation
                commonCount++;
            }
        }

        if (commonCount > 0) {
            result.set(String.valueOf(commonCount));
            context.write(key, result);
        }
    }
}
