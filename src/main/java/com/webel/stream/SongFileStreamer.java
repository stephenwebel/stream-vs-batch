package com.webel.stream;

import com.webel.common.Song;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Created by swebel on 1/10/2017.
 */
@Slf4j
class SongFileStreamer {
    static Stream<Song> stream(String pathToSongData) {
        try {
            BufferedReader songFileReader = new BufferedReader(new FileReader(pathToSongData));
            String currentLine = songFileReader.readLine();
            log.info("Headers for song data: {}", currentLine);
            return StreamSupport.stream(new SongFileSpliterator(songFileReader), false);
        } catch (IOException e) {
            log.error("Couldn't open song file from path: {}", pathToSongData, e);
        }
        return Stream.empty();
    }
}
