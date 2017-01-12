package com.webel.stream;

import com.webel.common.Song;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.StopWatch;

import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by swebel on 1/10/2017.
 */
@Slf4j
public class Main {
    private static final String pathToSongData = "src/main/resources/songdata.csv";
    private static final String pathToSongJson = "src/main/resources/songdata.json";
    private static final StopWatch stopWatch = new StopWatch();

    public static void main(String[] args) {
        log.info("Starting Stream Processing");
        stopWatch.start("Stream File Read");
        Stream<Song> songsFromFile = SongFileStreamer.stream(pathToSongData).peek(SongWriter::blockingWrite);
        Stream<Song> songWrittenToFile = SongWriter.stream(songsFromFile, pathToSongJson);
        Collection<Song> songs = songWrittenToFile.collect(Collectors.toList());
        stopWatch.stop();
        log.info("Found {} songs", songs.size());
        log.info(stopWatch.prettyPrint());
    }
}
