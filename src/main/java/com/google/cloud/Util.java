package com.google.cloud;

import com.google.cloud.bigquery.*;

import java.util.stream.Collectors;

/**
 * Created by bobo on 3/17/17.
 */
public class Util {

    private BigQuery bigquery;

    Util() {
        this.bigquery = new BigQueryOptions.DefaultBigqueryFactory().create(BigQueryOptions.defaultInstance());
    }


    public String tableName(String dataset, String table) {
        return dataset + "." + table + " ";
    }

    public String tableName(String dataset, String table, String shortCut) {
        return dataset + "." + table + " AS " + shortCut + " ";
    }

    // https://cloud.google.com/bigquery/querying-data#bigquery-sync-query-java
    public QueryResponse runQuery(String query, TableId destinationTable) throws Exception {

        double startTime = System.currentTimeMillis();

        QueryJobConfiguration configuration = QueryJobConfiguration.builder(query)
                .defaultDataset(DatasetId.of(destinationTable.dataset()))
                .destinationTable(destinationTable)
                .allowLargeResults(Boolean.TRUE)
                .flattenResults(Boolean.TRUE)
                .writeDisposition(JobInfo.WriteDisposition.WRITE_TRUNCATE) // overwrite
                .build();
        Job remoteJob = bigquery.create(JobInfo.of(configuration));
        remoteJob = remoteJob.waitFor();

        QueryResponse response = bigquery.getQueryResults(remoteJob.jobId());
        while (!response.jobCompleted()) {
            Thread.sleep(10000);
            response = bigquery.getQueryResults(response.jobId());
        }

        double endTime = System.currentTimeMillis();
        double totalTime = endTime - startTime;

        if (response.hasErrors()) {
            throw new RuntimeException(
                    response
                            .executionErrors()
                            .stream()
                            .<String>map(err -> err.message())
                            .collect(Collectors.joining("\n")));
        }

        System.out.printf("Table [%s.%s] created in %f seconds.%n", destinationTable.dataset(),
                destinationTable.table(),
                totalTime/1000);

        return response;
    }

    public QueryResponse runQuery(String query) throws Exception {

        double startTime = System.currentTimeMillis();

        QueryJobConfiguration configuration = QueryJobConfiguration.builder(query)
                .flattenResults(Boolean.TRUE)
                .writeDisposition(JobInfo.WriteDisposition.WRITE_TRUNCATE) // overwrite
                .build();
        Job remoteJob = bigquery.create(JobInfo.of(configuration));
        remoteJob = remoteJob.waitFor();

        QueryResponse response = bigquery.getQueryResults(remoteJob.jobId());
        while (!response.jobCompleted()) {
            Thread.sleep(10000);
            response = bigquery.getQueryResults(response.jobId());
        }

        double endTime = System.currentTimeMillis();
        double totalTime = endTime - startTime;

        if (response.hasErrors()) {
            throw new RuntimeException(
                    response
                            .executionErrors()
                            .stream()
                            .<String>map(err -> err.message())
                            .collect(Collectors.joining("\n")));
        }

        System.out.printf("Query [%s] executed in %f seconds.%n", query, totalTime/1000);

        return response;
    }
}
