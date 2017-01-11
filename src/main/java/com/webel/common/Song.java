package com.webel.common;

import lombok.Builder;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

/**
 * Created by swebel on 1/10/2017.
 */
@Value
@Builder
@Slf4j
public class Song {
    String artistName;
    String title;
    String link;
    String lyrics;
}
