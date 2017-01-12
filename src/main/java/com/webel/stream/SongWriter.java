package com.webel.stream;

import com.webel.common.Song;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Created by swebel on 1/11/2017.
 */
@Slf4j
public class SongWriter {

    static Stream<Song> stream(Stream<Song> songStream, String fileToWriteTo) {
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(fileToWriteTo));
            return StreamSupport.stream(new StreamBatchWriterSpliterator(songStream, writer, fileToWriteTo), false);
        } catch (IOException e) {
            log.error("Could not open file to write songs: {}", fileToWriteTo);
        }
        return Stream.empty();
    }

    public static void blockingWrite(Song song) {

    }
}
