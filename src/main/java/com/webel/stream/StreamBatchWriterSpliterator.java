package com.webel.stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.webel.common.Song;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by swebel on 1/11/2017.
 */
@Slf4j
public class StreamBatchWriterSpliterator extends Spliterators.AbstractSpliterator<Song> {
    private final static int BATCH_SIZE = 500;
    private final static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final Queue<Song> currentBatch = Lists.newLinkedList();
    private final Stream<Song> songs;
    private final BufferedWriter writer;
    private final String jsonFilePath;

    StreamBatchWriterSpliterator(Stream<Song> songs, BufferedWriter writer, String jsonFilePath) {
        super(57650, 0);
        this.songs = songs;
        this.writer = writer;
        this.jsonFilePath = jsonFilePath;
    }

    @Override
    public boolean tryAdvance(Consumer<? super Song> action) {
        Optional<Song> nextSong = getNextSong();
        if (nextSong.isPresent()) {
            action.accept(nextSong.get());
            return true;
        }
        return false;
    }

    private Optional<Song> getNextSong() {
        if (currentBatch.isEmpty()) {
            fetchNextBatch();
        }
        return Optional.ofNullable(currentBatch.poll());
    }

    private void fetchNextBatch() {
        List<Song> songsToWrite = songs.limit(BATCH_SIZE).collect(Collectors.toList());
        for (Song song : songsToWrite) {
            try {
                String songJson = OBJECT_MAPPER.writeValueAsString(song);
                writer.newLine();
                writer.write(songJson);
                currentBatch.offer(song);
            } catch (IOException e) {
                log.error("Could not write song to file: {}, song: {}", jsonFilePath, song);
            }
        }

        //if we did not create a full batch, songs is empty and we will no longer need to write
        if (songsToWrite.size() < BATCH_SIZE) {
            try {
                writer.close();
            } catch (IOException e) {
                log.error("Unable to close file writer");
            }
        }
    }
}
