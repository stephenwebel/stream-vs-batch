package com.webel.stream;

import com.google.common.collect.Lists;
import com.webel.common.Song;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.util.StopWatch;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by swebel on 1/11/2017.
 */
@Slf4j
@SpringBootApplication
@Configuration
public class StreamTestRunner {
    //    private static final String pathToSongData = "src/main/resources/songdata.csv";
//    private static final String pathToSongData = "src/main/resources/song.txt";
    private static final String pathToSongData = "src/main/resources/big_songs.txt";
    private static final String pathToSongJson = "src/main/resources/pureStreamWrite.json";
    private static final String pathToVolatileJson = "src/main/resources/volatileWrite.json";
    private static final StopWatch stopWatch = new StopWatch();
    private final NamedParameterJdbcTemplate namedTemplate;


    public StreamTestRunner(NamedParameterJdbcTemplate namedTemplate) {
        this.namedTemplate = namedTemplate;
    }

    void runStreamTest() {
//        runStreamReadTest();
//        runWriteTest();
//        runVolatileTest();
//        runDatabaseWrite();
        runDatabaseBatchWrite();
    }

    void runStreamReadTest() {
        log.info("Starting Stream Read Test");
        stopWatch.start("Stream Read");
        List<Song> songs = SongFileStreamer.stream(pathToSongData).collect(Collectors.toList());
        log.info("read {} songs from {}", songs.size(), pathToSongData);
        log.info("first song: {}", songs.get(0));
        log.info("last song: {}", songs.get(songs.size() - 1));
        stopWatch.stop();
        log.info("{}", stopWatch.prettyPrint());
    }

    void runWriteTest() {
        log.info("Starting Stream Write Test");
        stopWatch.start("Stream Read -> Stream Write");
        Collection<Song> songs = Lists.newArrayList();
        try (StreamWriter streamWriter = new StreamWriter(pathToSongJson, namedTemplate)) {
            songs = SongFileStreamer.stream(pathToSongData)
                    .peek(streamWriter::writeSong)
                    .collect(Collectors.toList());
        } catch (IOException e) {
            log.error("", e);
        }
        stopWatch.stop();
        log.info("Found {} songs", songs.size());
        log.info(stopWatch.prettyPrint());
    }

    void runVolatileTest() {
        log.info("Starting Stream With Volatile Write");
        stopWatch.start("Streaming Volatile Write");
        long numSongs = 0L;
        try (StreamWriter streamWriter = new StreamWriter(pathToVolatileJson, namedTemplate)) {
            numSongs = SongFileStreamer.stream(pathToSongData)
                    .map(streamWriter::volatileWrite)
                    .filter(Objects::nonNull)
                    .count();
        } catch (IOException e) {
            log.error("", e);
        }
        stopWatch.stop();
        log.info("Found {} songs", numSongs);
        log.info(stopWatch.prettyPrint());
    }

    void runDatabaseBatchWrite() {
        log.info("Starting Stream Jdbc Batch Processing");
        stopWatch.start("Stream Read, Batching Write");
        long numSongs = 0L;
        try (StreamWriter streamWriter = new StreamWriter(pathToSongJson, namedTemplate)) {
            log.info("Stream writer is initialized");
            numSongs = SongFileStreamer.stream(pathToSongData)
                    .peek(streamWriter::batchInsertSong)
                    .count();
        } catch (IOException e) {
            log.error("", e);
        }
        stopWatch.stop();
        log.info("Found {} songs", numSongs);
        log.info(stopWatch.prettyPrint());
    }

    void runDatabaseWrite() {
        log.info("Starting Stream Jdbc Processing");
        stopWatch.start("Stream Write");
        long numSongs = 0L;
        try (StreamWriter streamWriter = new StreamWriter(pathToSongJson, namedTemplate)) {
            numSongs = SongFileStreamer.stream(pathToSongData)
                    .peek(streamWriter::insertSong)
                    .count();
        } catch (IOException e) {
            log.error("", e);
        }
        stopWatch.stop();
        log.info("Found {} songs", numSongs);
        log.info(stopWatch.prettyPrint());
    }
}
