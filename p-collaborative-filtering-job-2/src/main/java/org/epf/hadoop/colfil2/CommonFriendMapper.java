package org.epf.hadoop.colfil2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CommonFriendMapper extends Mapper<Object, Text, UserPair, Text> {
    private UserPair userPair = new UserPair();
    private Text relation = new Text();


    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Input line: user <TAB> friends (e.g., "adamavery\tabigailallan,abigailmorgan,adamchurchill")
        String[] line = value.toString().split("\t");
        if (line.length != 2) {
            return; // Skip invalid lines
        }

        String user = line[0].trim();
        String[] friends = line[1].split(",");

        // Generate pairs for the user's friends
        for (int i = 0; i < friends.length; i++) {
            for (int j = i + 1; j < friends.length; j++) {
                userPair.set(friends[i].trim(), friends[j].trim());
                relation.set(user);
                context.write(userPair, relation);
            }
        }
    }
}
