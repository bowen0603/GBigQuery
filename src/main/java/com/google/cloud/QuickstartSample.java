package com.google.cloud;

// Imports the Google Cloud client library
//import com.google.api.services.bigquery.Bigquery;
//import com.google.api.services.bigquery.model.*;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableId;

/**
 * Key steps to set up:
 * 1. Install client library: https://cloud.google.com/bigquery/docs/reference/libraries#client-libraries-install-java
 * 2. Create the project with maven repository with needed libraries
 * 3. Install and authenticate Google Cloud SDK: gcloud beta auth application-default login
 * 4. Import proper libraries to Intellij
 *
 * Basic tasks needed:
 * 1. Execute queries, and read into a permanent table。
 * 2. Be able to wait until the end of one job, then continue another one. (https://cloud.google.com/bigquery/querying-data#asyncqueries)
 *
 *
 * Problem: two sets of APIs??? which one to use???
 * 1. com.google.cloud.bigquery
 * 2. com.google.api.services.bigquery
 *
 * TODO to watch out when writing the code:
 * 1. add an extra space at the end of a statement for the query validation.
 * 2. add the month unit for the table of each table to create separate table sets.
 */
public class QuickstartSample {

    private BigQuery bigquery;
    private String defaultDataset;
    private int timeIntervalUnit; // in months


    QuickstartSample() {
        timeIntervalUnit = 1;
        defaultDataset = "bowen_quitting_script";
        this.bigquery = new BigQueryOptions.DefaultBigqueryFactory().create(BigQueryOptions.defaultInstance());
    }

    public static void main(String... args) throws Exception {
        QuickstartSample self = new QuickstartSample();

        // Create the valid range of the project in the following three steps
//        self.createTimeRangeOfAllProjects();
//        self.createFullTimeIntervalFile();
//        self.createValidTimeRangeForProjects();


        // Create longitudinal data for DVs
        self.createLongitudinalDVs();


        // Create longitudinal data for IVs


        System.out.println("Done .. ");
    }

    /**
     * Identify newcomers, leavers, and remaining members in the project.
     * - create editor-wp-tcount table
     */
    private void identifyMembersInProjects() {
        // starting from table: (Find the first and last time interval for each editor on wikiprojects (6-month interval))
        // in file Queries and Tables
    }

    /**
     * Compute DVs in the each time interval for each wikiproject
     */
    private void createLongitudinalDVs() throws Exception {

        // DV - coordination: total number of coordination in ns 45 of each project in each time interval
        runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                "t2.tcount AS tcount," +
                "COUNT(*) AS project_coordination," +
                "FROM " + tableName("bowen_wikis_quitting", "rev_ns45_user_wikiproject_ts_encoded_full", "t1") +
                "INNER JOIN " + tableName(defaultDataset, "script_user_wp_revs_45_valid_users_wps_valid_range"+timeIntervalUnit, "t2") +
                "ON t1.nwikiproject = t2.nwikiproject " +
                "WHERE t1.timestamp >= t2.start_ts AND t1.timestamp < t2.end_ts " +
                "GROUP BY nwikiproject, tcount",
                TableId.of(defaultDataset, "dv_wp_coor45_per_time_interval_months"+timeIntervalUnit));

        // fill the values for the missing time intervals
        runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                "t1.tcount AS tcount," +
                "IF (t2.nwikiproject IS NULL AND t2.tcount IS NULL, 0, t2.project_coordination) AS project_coors," +
                "FROM " + tableName(defaultDataset, "script_user_wp_revs_45_valid_users_wps_valid_range"+timeIntervalUnit, "t1") +
                "LEFT JOIN " + tableName(defaultDataset, "dv_wp_coor45_per_time_interval_months"+timeIntervalUnit, "t2") +
                "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount " +
                "ORDER BY nwikiproject, tcount", TableId.of(defaultDataset, "dv_wp_full_coor45_per_time_interval_months"+timeIntervalUnit));

        // DV -  productivity: total article edits of the members
        runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t2.tcount AS tcount," +
                        "COUNT(*) AS group_article_productivity," +
                        "FROM " + tableName("bowen_wikis_quitting", "rev_ns0_member_project_edits", "t1") +
                        "INNER JOIN " + tableName(defaultDataset, "script_user_wp_revs_45_valid_users_wps_valid_range" + timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject " +
                        "WHERE t1.timestamp >= t2.start_ts AND t1.timestamp AS t2.end_ts " +
                        "GROUP BY nwikiproject, tcount " +
                        "ORDER BY nwikiproject, tcount",
                TableId.of(defaultDataset, "dv_wp_mbr_prod0_per_time_interval_months" + timeIntervalUnit));

        // fill the values for the missing time intervals
        runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "IF (t2.nwikiproject IS NULL AND t2.tcount IS NULL, 0, t2.group_article_productivity) AS project_prod," +
                        "FROM " + tableName(defaultDataset, "script_user_wp_revs_45_valid_users_wps_valid_range"+timeIntervalUnit, "t1") +
                        "LEFT JOIN " + tableName(defaultDataset, "dv_wp_mbr_prod0_per_time_interval_months" + timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount " +
                        "ORDER BY nwikiproject, tcount",
                TableId.of(defaultDataset, "dv_wp_full_mbr_prod0_per_time_interval_months" + timeIntervalUnit));

        // DV: article communication
        // TODO: need to check the alignment of time intervals
        // TODO: check rev_ns1_member_project_edits table - article edits??? (confusing name)
        runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t2.tcount AS tcount," +
                        "COUNT(*) AS art_comm," +
                        "FROM " + tableName("bowen_wikis_quitting", "rev_ns1_member_project_edits", "t1") +
                        "INNER JOIN " + tableName(defaultDataset, "script_user_wp_revs_45_valid_users_wps_valid_range" + timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject " +
                        "WHERE t1.timestamp >= t2.start_ts AND t1.timestamp < t2.end_ts " +
                        "GROUP BY nwikiproject, tcount " +
                        "ORDER BY nwikiproject, tcount",
                TableId.of(defaultDataset, "dv_wp_art_comm1_per_time_interval_months"+timeIntervalUnit));

        // fill the values for the missing time intervals
        runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "IF (t2.nwikiproject IS NULL AND t2.tcount IS NULL, 0, t2.art_comm) AS project_art_comm," +
                        "FROM " + tableName(defaultDataset, "script_user_wp_revs_45_valid_users_wps_valid_range" + timeIntervalUnit, "t1") +
                        "LEFT JOIN " + tableName(defaultDataset, "dv_wp_art_comm1_per_time_interval_months" + timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount " +
                        "ORDER BY nwikiproject, tcount",
                TableId.of(defaultDataset, "dv_wp_full_art_comm1_per_time_interval_months"+timeIntervalUnit));

        // DV: user communication
        runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t2.tcount AS tcount," +
                        "COUNT(*) AS user_comm," +
                        "FROM " + tableName("bowen_wikis_quitting", "lng_member_wp_talk_page_edits_ts_6months", "t1") +
                        "INNER JOIN " + tableName(defaultDataset, "script_user_wp_revs_45_valid_users_wps_valid_range" + timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject " +
                        "WHERE t1.timestamp >= t2.start_ts AND t1.timestamp < t2.end_ts " +
                        "GROUP BY nwikiproject, tcount " +
                        "ORDER BY nwikiproject, tcount",
                TableId.of(defaultDataset, "dv_wp_user_comm3_per_time_interval_months" + timeIntervalUnit));

        // fill the values for the missing time intervals
        runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "IF (t2.nwikiproject IS NULL AND t2.tcount IS NULL, 0, t2.user_comm) AS project_user_comm," +
                        "FROM " + tableName(defaultDataset, "script_user_wp_revs_45_valid_users_wps_valid_range" + timeIntervalUnit, "t1") +
                        "LEFT JOIN " + tableName(defaultDataset, "dv_wp_user_comm3_per_time_interval_months" + timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount " +
                        "ORDER BY nwikiproject, tcount",
                TableId.of(defaultDataset, "dv_wp_full_user_comm3_per_time_interval_months" + timeIntervalUnit));
    }

    private void createTimeRangeOfAllProjects() throws Exception {

        // merge ns 4 and 5
        runQuery("SELECT *" +
                 "FROM bowen_wikis_quitting.rev_ns4_user_wikiproject_ts, bowen_wikis_quitting.rev_ns5_user_wikiproject_ts",
                TableId.of(defaultDataset, "script_rev_ns45_user_wp_ts"));


        // select valid editors
        runQuery("SELECT t1.user_text AS user_text," +
                        "t2.user_id AS user_id," +
                        "t1.wikiproject AS wikiproject," +
                        "t1.timestamp AS timestamp," +
                        "t1.ns AS ns," +
                        "FROM " + defaultDataset + "." + "script_rev_ns45_user_wp_ts AS t1 " +
                        "INNER JOIN bowen_editor_attachments.users_valid_withIds AS t2 " +
                        "ON t1.user_text = t2.user_text",
                TableId.of(defaultDataset, "script_user_wp_revs_45_valid_users"));

        // select valid project
        runQuery("SELECT t1.user_text AS user_text," +
                        "t1.user_id AS user_id," +
                        "t1.wikiproject AS wikiproject," +
                        "t2.nwikiproject AS nwikiproject," +
                        "t1.timestamp AS timestamp," +
                        "t1.ns AS ns," +
                        "FROM " + defaultDataset + "." + "script_user_wp_revs_45_valid_users AS t1 " +
                        "INNER JOIN bowen_editor_attachments.wikiprojects_valid_withIds AS t2 " +
                        "ON t1.wikiproject = t2.wikiproject",
                TableId.of(defaultDataset, "script_user_wp_revs_45_valid_users_wps"));

        // find max and min timestamps
        runQuery("SELECT MAX(timestamp) AS maxTS," +
                        "MIN(timestamp) AS minTS," +
                        "FROM " + defaultDataset + "." + "script_user_wp_revs_45_valid_users_wps",
                TableId.of(defaultDataset, "script_ts_range_revs_45"));


        /*
         *   given the max and min range of the dataset, create the range split by X months
         *   need to manually input the max and min timestamp based on the previous query result
         */
        createFullTimeIntervalFile();

        // TODO: need to manually upload the table to Bigquery

        /*
         * Need to manually upload the table created the last function before
         * running the following function.
         */
        createValidTimeRangeForProjects();
    }

    // TODO work on this, or use it as a start of another logic flow
    private void createValidTimeRangeForProjects() throws Exception {
        // select the time range of each wikiproject based on time range of ns 4 and 5
        runQuery("SELECT nwikiproject," +
                "MAX(timestamp) AS maxTS," +
                "MIN(timestamp) AS minTS," +
                "FROM " + tableName(defaultDataset, "script_user_wp_revs_45_valid_users_wps") +
                "GROUP BY nwikiproject", TableId.of(defaultDataset, "script_wp_revs_45_time_range"));

        // create valid project time range
        runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t2.tcount AS tcount," +
                        "t2.start_ts AS start_ts," +
                        "t2.end_ts AS end_ts," +
                        "FROM " + tableName(defaultDataset, "script_wp_revs_45_time_range", "t1") +
                        "INNER JOIN " + tableName(defaultDataset, "full_time_intervals_" + timeIntervalUnit + "month", "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject " +
                        "WHERE t1.minTS < t2.end_ts AND t1.maxTS > t2.start_ts",
                TableId.of(defaultDataset, "script_user_wp_revs_45_valid_users_wps_valid_range"+timeIntervalUnit));

        // TODO: may want to add the starting tcount and ending tcount for each project
    }

    private void logic() {

        /**
         * 1. Compute the time range of all the projects based on ns 4 or 5.
         * 2. Split by 1 or 2 months for each project on the timeline.
         * 3. Join with editing tables of editors for the computation of the variables of each time interval
         *
         * Breakdowns:
         * 1. generate DVs first - need to create new lng_wikiproject_prod_per_time_interval_6months table for X months
         *      - DV - WikiProject Productivity Change per time interval
         */


    }

    private void createFullTimeIntervalFile() {
        int maxTS = 1433341432;
        int minTS = 1001492429, startTS = minTS;

        // idea: add in more projects 0 - 2000 for the entire range, later do inner join to remove redundant projects
        // output: nwikiproject, ts_start, ts_end, tcount
        List<Integer> timeRange = new LinkedList<Integer>();
        while (startTS < maxTS) {
            timeRange.add(startTS);
            startTS += 3600 * 24 * 30 * timeIntervalUnit;
        }
        timeRange.add(maxTS);

        try{
            PrintWriter writer = new PrintWriter(String.format("full_time_intervals%d.csv", timeIntervalUnit), "UTF-8");
            writer.printf("nwikiproject,start_ts,end_ts,tcount\n");
            for (int id = 0; id <= 2000; ++id) {
                for (int i = 0; i < timeRange.size()-1; ++i) {
                    writer.printf("%d,%d,%d,%d\n", id, timeRange.get(i), timeRange.get(i+1),(i+1));
                }
            }
            writer.close();
        } catch (IOException e) {
            System.out.println("Having errors when writing out to the file.");
        }
    }

    private String tableName(String dataset, String table) {
        return dataset + "." + table + " ";
    }

    private String tableName(String dataset, String table, String shortCut) {
        return dataset + "." + table + " AS " + shortCut + " ";
    }

    // https://cloud.google.com/bigquery/querying-data#bigquery-sync-query-java
    private void runQuery(String query, TableId destinationTable) throws Exception {

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
    }
}
