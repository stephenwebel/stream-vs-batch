package com.webel.stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.webel.common.Song;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by stephenwebel1 on 1/11/17.
 */
@Slf4j
class StreamWriter implements AutoCloseable {

    private static int SIZE = 0;
    private static final int BATCH_SIZE = 10;
    private static final String INSERT_SONG = "insert " +
            "into music_library.songz " +
            "(artist, title, link, lyrics) " +
            "values " +
            "(:artist, :title, :link, :lyrics)";
    private final FileWriter writer;
    private final NamedParameterJdbcTemplate namedTemplate;
    private final String pathToSongJson;
    private ObjectMapper objectMapper = new ObjectMapper();
    private List<Map<String, ?>> parameters = Lists.newArrayList();


    private StringBuilder batch = new StringBuilder();

    public StreamWriter(String pathToSongJson, NamedParameterJdbcTemplate namedTemplate) throws IOException {
        this.pathToSongJson = pathToSongJson;
        writer = new FileWriter(pathToSongJson, true);
        this.namedTemplate = namedTemplate;
    }

    void writeSong(Song song) {
        try (FileWriter singleSongWriter = new FileWriter(pathToSongJson, true)) {
            singleSongWriter.write(objectMapper.writeValueAsString(song) + "\n");
        } catch (IOException e) {
            log.error("", e);
        }
    }

    Song volatileWrite(Song song) {
        try {
            writer.write(objectMapper.writeValueAsString(song) + "\n");
            return song;
        } catch (IOException e) {
            log.warn("could not write song: {}", song);
        }
        return null;
    }

    void insertSong(Song song) {
        Map<String, Object> parameters = Maps.newHashMap();
        parameters.put("artist", song.getArtistName());
        parameters.put("title", song.getTitle());
        parameters.put("link", song.getLink());
        parameters.put("lyrics", song.getLyrics());
        namedTemplate.update(INSERT_SONG, parameters);
    }


    void batchInsertSong(Song song) {
        Map<String, Object> parameterMap = Maps.newHashMap();
        parameterMap.put("artist", song.getArtistName());
        parameterMap.put("title", song.getTitle());
        parameterMap.put("link", song.getLink());
        parameterMap.put("lyrics", song.getLyrics());
        parameters.add(parameterMap);
        if (parameterMap.size() >= BATCH_SIZE) {
            Map[] paramArr = new Map[parameters.size()];
            namedTemplate.batchUpdate(INSERT_SONG, parameters.toArray(paramArr));
            parameters = Lists.newArrayListWithCapacity(BATCH_SIZE);
        }

    }

    void batchWrite(Song song) {
        try {
            ++SIZE;
            //this is dumb
            batch.append(objectMapper.writeValueAsString(song)).append("\n");
            if (SIZE % BATCH_SIZE == 0) {
                writer.write(batch.toString());
            }
        } catch (IOException e) {
            log.error("", e);
        }
    }

    public void close() throws IOException {
        if (!parameters.isEmpty()) {
            Map[] paramArr = new Map[parameters.size()];
            namedTemplate.batchUpdate(INSERT_SONG, parameters.toArray(paramArr));
        }
        writer.close();
    }
}