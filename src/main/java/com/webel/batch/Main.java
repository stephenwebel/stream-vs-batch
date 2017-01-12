package com.webel.batch;

import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.util.StopWatch;

import java.io.IOException;

/**
 * Created by swebel on 1/10/2017.
 */
@Slf4j
public class Main {
    private static final String pathToSongData = "src/main/resources/songdata.csv";
    private static final StopWatch stopWatch = new StopWatch();

    public static void main(String[] args) throws IOException {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        //MySQL database we are using
        dataSource.setDriverClassName("com.mysql.jdbc.Driver");
        dataSource.setUrl("jdbc:mysql://localhost:3306/");//change url
        dataSource.setUsername("java_owner");//change userid
        dataSource.setPassword("java_pw");//change pwd

        NamedParameterJdbcTemplate namedTemplate = new NamedParameterJdbcTemplate(dataSource);

        BatchTestRunner batchTestRunner = new BatchTestRunner(namedTemplate);
        batchTestRunner.runTest();

    }
}
