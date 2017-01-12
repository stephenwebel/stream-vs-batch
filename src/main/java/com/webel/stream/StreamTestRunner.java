package com.webel.stream;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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
    private static final String pathToSongData = "src/main/resources/songdata.csv";
    private static final String pathToSongJson = "src/main/resources/write.json";
    private static final String pathToBatchJson = "src/main/resources/batchWrite.json";
    private static final String pathToVolatileJson = "src/main/resources/volatileWrite.json";
    private static final StopWatch stopWatch = new StopWatch();
    private final NamedParameterJdbcTemplate namedTemplate;


    public StreamTestRunner(NamedParameterJdbcTemplate namedTemplate) {
        this.namedTemplate = namedTemplate;
    }

    void runStreamTest() {
        log.info("Starting Stream Processing");
        runDatabaseWrite();
        runDatabaseBatchWrite();
//        runBatchTest();
//        runWriteTest();
//        runVolitileTest();
    }

    private void runDatabaseBatchWrite() {
        log.info("Starting Stream Jdbc Batch Processing");
        stopWatch.start("Stream Write");
        Collection<Song> songs = Lists.newArrayList();
        try (StreamFileWriter streamFileWriter = new StreamFileWriter(pathToSongJson, namedTemplate)) {
            songs = SongFileStreamer.stream(pathToSongData)
                    .peek(streamFileWriter::batchInsertSong)
                    .collect(Collectors.toList());
        } catch (IOException e) {
            log.error("", e);
        }
        stopWatch.stop();
        log.info("Found {} songs", songs.size());
        log.info(stopWatch.prettyPrint());
    }

    public void runDatabaseWrite() {
        log.info("Starting Stream Jdbc Processing");
        stopWatch.start("Stream Write");
        Collection<Song> songs = Lists.newArrayList();
        try (StreamFileWriter streamFileWriter = new StreamFileWriter(pathToSongJson, namedTemplate)) {
            songs = SongFileStreamer.stream(pathToSongData)
                    .peek(streamFileWriter::insertSong)
                    .collect(Collectors.toList());
        } catch (IOException e) {
            log.error("", e);
        }
        stopWatch.stop();
        log.info("Found {} songs", songs.size());
        log.info(stopWatch.prettyPrint());
    }

    public void runWriteTest() {
        log.info("Starting Stream Processing");
        stopWatch.start("Stream Write");
        Collection<Song> songs = Lists.newArrayList();
        try (StreamFileWriter streamFileWriter = new StreamFileWriter(pathToSongJson, namedTemplate)) {
            songs = SongFileStreamer.stream(pathToSongData)
                    .peek(streamFileWriter::writeSong)
                    .collect(Collectors.toList());
        } catch (IOException e) {
            log.error("", e);
        }
        stopWatch.stop();
        log.info("Found {} songs", songs.size());
        log.info(stopWatch.prettyPrint());
    }

    public void runBatchTest() {
        log.info("Starting Stream With Batch Write");
        stopWatch.start("Stream Batch Write");
        Collection<Song> songs = Lists.newArrayList();
        try (StreamFileWriter streamFileWriter = new StreamFileWriter(pathToBatchJson, namedTemplate)) {
            songs = SongFileStreamer.stream(pathToSongData)
                    .peek(streamFileWriter::batchWrite)
                    .collect(Collectors.toList());
        } catch (IOException e) {
            log.error("", e);
        }
        stopWatch.stop();
        log.info("Found {} songs", songs.size());
        log.info(stopWatch.prettyPrint());
    }

    public void runVolitileTest() {
        log.info("Starting Stream With Volatile Write");
        stopWatch.start("Streaming Volatile Write");
        Collection<Song> songs = Lists.newArrayList();
        try (StreamFileWriter streamFileWriter = new StreamFileWriter(pathToVolatileJson, namedTemplate)) {
            songs = SongFileStreamer.stream(pathToSongData)
                    .map(streamFileWriter::volatileWrite)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
        } catch (IOException e) {
            log.error("", e);
        }
        stopWatch.stop();
        log.info("Found {} songs", songs.size());
        log.info(stopWatch.prettyPrint());
    }
}
