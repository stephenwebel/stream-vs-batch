package com.webel.common;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * Created by swebel on 1/10/2017.
 */
public class SongArgumentParser {

//    public static final String pathToSongData = "src/main/resources/songdata.csv";
//    public static final String pathToSongData = "src/main/resources/song.txt";
    public static final String pathToSongData = "src/main/resources/big_songs.txt";

    public static List<String> parseArgumentLine(String line) {
        List<String> result = Lists.newArrayList();
        if (line == null || line.isEmpty()) {
            return result;
        }
        boolean inQuotes = false;
        StringBuilder curArg = new StringBuilder();
        for (char ch : line.toCharArray()) {
            if (result.size() > 2) {
                if (ch != '"') {
                    curArg.append(ch);
                }
            } else if (inQuotes && ch == '"') {
                inQuotes = false;
            } else if (!inQuotes && ch == '"') {
                inQuotes = true;
            } else if (!inQuotes && ch == ',') {
                result.add(curArg.toString());
                curArg = new StringBuilder();
            } else {
                curArg.append(ch);
            }
        }
        result.add(curArg.toString());
        return result;
    }
}
