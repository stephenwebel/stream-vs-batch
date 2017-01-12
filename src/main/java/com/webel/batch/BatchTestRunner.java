package com.webel.batch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.webel.common.Song;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.util.StopWatch;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * Created by stephenwebel1 on 1/12/17.
 */
@Slf4j
public class BatchTestRunner {
    private static final String pathToSongData = "src/main/resources/songdata.csv";
    private static final String pathToSongJson = "src/main/resources/pureBatchWrite.json";
    private static final StopWatch stopWatch = new StopWatch();
    private ObjectMapper objectMapper = new ObjectMapper();
    private final NamedParameterJdbcTemplate namedTemplate;


    public BatchTestRunner(NamedParameterJdbcTemplate namedTemplate) throws IOException {
        this.namedTemplate = namedTemplate;
    }

    void runTest() {
//        runBatchReadTest();
        runBatchReadFileWriteTest();
    }

    public void runBatchReadTest() {
        log.info("Starting Batch Read Test");
        stopWatch.start("Batch File Read");
        //load all songs into memory
        List<Song> songs = new SongFileReader(pathToSongData).readSongsFromFile();
        log.info("read {} songs from {}", songs.size(), pathToSongData);
        log.info("first song: {}", songs.get(0));
        log.info("last song: {}", songs.get(songs.size() - 1));
        stopWatch.stop();
        log.info("{}", stopWatch.prettyPrint());
    }

    public void runBatchReadFileWriteTest() {
        log.info("Starting Batch Processing");
        stopWatch.start("Batch File Read -> Batch Write");
        //load all songs into memory
        Collection<Song> songs = new SongFileReader(pathToSongData).readSongsFromFile();
        log.info("found {} songs", songs.size());
        //create the full file to write
        StringBuilder songString = new StringBuilder();
        for (Song song : songs) {
            try {
                songString.append(objectMapper.writeValueAsString(song)).append("\n");
            } catch (JsonProcessingException e) {
                log.error("", e);
            }
        }
        //write the string
        try (FileWriter writer = new FileWriter(pathToSongJson, false)) {
            writer.write(songString.toString());
        } catch (IOException e) {
            log.error("", e);
        }
        stopWatch.stop();
        log.info(stopWatch.prettyPrint());
    }
}
