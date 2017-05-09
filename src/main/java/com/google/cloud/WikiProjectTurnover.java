package com.google.cloud;

import java.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import com.google.cloud.bigquery.*;
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
public class WikiProjectTurnover {

    private int recordThreshold;
    private Util util;
    private String defaultDataset;
    private String superficialLeaverAnalysisFile;
    private int timeIntervalUnit; // in months
    private int monthlyEditingThreshold;
    // number of edits on articles within the scope of the project


    WikiProjectTurnover() {
        recordThreshold = 500;
        timeIntervalUnit = 3;
        monthlyEditingThreshold = 5;
//        superficialLeaverAnalysisFile = "superficial_leaver_analysis_ns45_above_0.1_0.2.csv";
        superficialLeaverAnalysisFile = "substantive_leaver_analysis_ns45_0.15_0.2.csv";
        defaultDataset = "bowen_quitting_script";
        this.util = new Util();
    }


    /**
     * * Logic of analysis:
     * 1. group composition - purely based on the number of each types of members
     * ** Done : identifyMembersInProjects()
     * ************************************************************************************
     * 2. member experience - based on editor's behaviors on the previous time interval
     * key tables: (1) user-wp-tcount, (2) user-ns-edits history
     * table (1): script_user_wp_active_range_revs45
     * table (2): should be generated already, need to relocate those tables (revision tables)
     *
     *
     * ************************************************************************************
     * Key functions, tables, and operationalizations
     * *** 1. (user, wp, active range) table
     * A key table contains the information of editor's active period on each project.
     * the time interval is calculated based on the initial setting of time range.
     * Editor's active range on the project is estimated by the edits on ns 4 and 5 which is questionable still.
     * todo: Might need better rational on this?
     * table: script_user_wp_active_range_revs45
     *
     * *** 2. (wp, tcount) table
     * table: script_user_wp_revs_45_valid_users_wps_valid_range
     *
     * *** 3. (user, wp, full active tcounts) table
     * table: "script_user_wp_active_range_revs45" + timeIntervalUnit + "edit" + monthlyEditingThreshold, "t1"
     *
     * *** 4. (user, wp, wp_start_tcount, wp_end_tcount) table
     * table: "script_user_wp_first_last_tcounts" + timeIntervalUnit + "edit" + monthlyEditingThreshold, "t1"
     *
     */


    public static void main(String... args) throws Exception {
        WikiProjectTurnover self = new WikiProjectTurnover();
        
        
        // Create the valid range of the project in the following three steps
//        self.createTimeRangeOfAllProjects();
//        self.createFullTimeIntervalFile();
//        self.createValidTimeRangeForProjects();


        // Create longitudinal data for DVs
//        self.createLongitudinalProjectLevelDVs();


        // Create longitudinal data for IVs
//        self.identifyMembersInProjects();

        // Merge the above variables generated into one table
//        self.mergeDVsAndMemberComposition();


        // Compute advanced member attributes
//        self.computeAdvancedMemberAttributes();
//        self.relocateEditorActiveRangeOnProjects();
//        self.analysisOnLeavingStayings();

//        self.addAverageLeaverStayingsToResults();

//        self.createLongitudinalProjectLevelDVs();

//        self.mergeGroupDVsIntoFullTable();



//        self.identifySpecialSuperficialLeaversOnArtcilesWPTcount();
//        self.leaverLagEffectsanalysis();

//        self.identifySepcialSuperficialsOnProjectPageWPTcount();
//        self.identifySepcialSubstantivesOnProjectPageWPTcount();

//        self.addSuperficialLastTcountEditsToResults();

//        self.addProjectDeltaDVsToResults();

//        self.addLeaversAccumulatedProjectEditsInEachTcountWithinWPToResults();

//        self.addProjectDeltaDVsToResults();


//        self.addLeaversAccumulatedProjectEditsInEachTcountWithinWPToResults();
//
//        self.identifyShortAndLongTermLeavers();
//        self.identityWikiProjectNewbies();

//        self.addNewcomerLeaverPriorExperienceToResults();
//        self.addNewcomerAccumulatedWikipediaEditsInEachTcountWithinWPToResults();
//        self.allWikipediaUserSurvivialAnalysis();


//        self.analysisOnEditorActivityAfterJoining();

//        self.createProjectCreationPeriod();

        self.fetchArticleTitles();

        System.out.println("Done .. ");
    }

    private void fetchArticleTitles() throws Exception {

//        util.runQuery("select rev_page_title as article_name, " +
//                            "rev_user_id as user_id, " +
//                            "rev_timestamp, " +
//                            "from " +
//                            util.tableName("bowen_user_dropouts", "revs") +
//                            "where ns in (0, 1, 3, 4, 5)",
//                    TableId.of("ExperiencedNewcomers", "revs_articlenames_subset_01345"));
//
//        util.runQuery("select *, " +
//                        "3 as ns, " +
//                        "from [robert-kraut-1234:ExperiencedNewcomers.wikiproject_revs_3_valid_users_valid_proj]",
//                TableId.of("ExperiencedNewcomers", "wp_revs_3_valid_users_valid_proj"));
//
//        util.runQuery("select user_id, " +
//                            "nwikiproject, " +
//                            "wikiproject, " +
//                            "timestamp, " +
//                            "ns," +
//                            "from " +
//                            "[robert-kraut-1234:ExperiencedNewcomers.wikiproject_revs_0145_valid_users_valid_proj], " +
//                            "[robert-kraut-1234:ExperiencedNewcomers.wp_revs_3_valid_users_valid_proj]",
//                TableId.of("ExperiencedNewcomers", "revs_01345_valid_users_valid_proj"));

        // select a subset of users
//        util.runQuery("SELECT UNIQUE(user_id) AS user_id," +
//                        "FROM " + util.tableName("ExperiencedNewcomers", "revs_01345_valid_users_valid_proj"),
//                TableId.of("ExperiencedNewcomers", "unique_users"));
//
//        util.runQuery("SELECT user_id," +
//                            "RAND(1000) AS rand," +
//                            "FROM " + util.tableName("ExperiencedNewcomers", "unique_users") +
//                            "ORDER BY rand " +
//                            "LIMIT 1000",
//                TableId.of("ExperiencedNewcomers", "random_subset_users1000"));


        util.runQuery("SELECT t1.user_id AS user_id," +
                        "t1.nwikiproject AS nwikiproject," +
                        "t1.wikiproject AS wikiproject," +
                        "t1.timestamp AS timestamp," +
                        "t1.ns AS ns," +
                        "FROM " + util.tableName("ExperiencedNewcomers", "revs_01345_valid_users_valid_proj", "t1") +
                        "INNER JOIN " + util.tableName("ExperiencedNewcomers", "random_subset_users1000", "t2") +
                        "ON t1.user_id = t2.user_id",
                TableId.of("ExperiencedNewcomers", "revs_01345_valid_users_valid_proj_sampled_users"));

        util.runQuery("select * from " +
                        "[robert-kraut-1234:ExperiencedNewcomers.revs_01345_valid_users_valid_proj_sampled_users] " +
                        " order by user_id, nwikiproject, timestamp",
                TableId.of("ExperiencedNewcomers", "revs_01345_valid_users_valid_proj_ordered"));

        util.runQuery("select * from (select *, ROW_NUMBER() OVER (PARTITION BY user_id, nwikiproject " +
                "ORDER BY user_id, nwikiproject, timestamp) as pos FROM " +
//         "[robert-kraut-1234:ExperiencedNewcomers.revs_01345_valid_users_valid_proj] " +
                        "[robert-kraut-1234:ExperiencedNewcomers.revs_01345_valid_users_valid_proj_ordered] " +
                "order by user_id, nwikiproject, timestamp) where pos <= 10",
                TableId.of("ExperiencedNewcomers", "revs_01345_valid_users_valid_proj_first10edits"));



        util.runQuery("select t1.user_id as user_id, t1.nwikiproject as nwikiproject, " +
                "t1.wikiproject as wikiproject, t2.article_name, t1.timestamp as timestamp, " +
                "FORMAT_UTC_USEC(t1.timestamp) as " +
                "hr_timestamp FROM " +
        util.tableName("ExperiencedNewcomers", "revs_01345_valid_users_valid_proj_first10edits", "t1")
        + " inner join " +
        util.tableName("ExperiencedNewcomers", "revs_articlenames_subset_01345", "t2") +
        " on t1.user_id = t2.user_id and t1.timestamp = t2.rev_timestamp",
        TableId.of("ExperiencedNewcomers", "top5edits_articlenames_hr_timestamps"));
    }


    private void createProjectCreationPeriod() throws Exception {

//        // locate the tcount of the first edit timestamp
//        util.runQuery("SELECT nwikiproject," +
//                        "MIN(tcount) AS creation_tcount," +
//                        "FROM " + util.tableName(defaultDataset, "script_user_wp_revs_45_valid_users_wps_valid_range"+timeIntervalUnit) +
//                        "GROUP BY nwikiproject",
//                TableId.of(defaultDataset, "project_creation_date_tcount"+timeIntervalUnit));
//
//        // identify valid projects
//        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
//                        "t1.creation_tcount AS creation_tcount," +
//                        "FROM " + util.tableName(defaultDataset, "project_creation_date_tcount"+timeIntervalUnit, "t1") +
//                        "INNER JOIN " + util.tableName(defaultDataset, "wp_valid_tcounts", "t2") +
//                        "ON t1.nwikiproject = t2.nwikiproject",
//                TableId.of(defaultDataset, "validproject_creation_date_tcount"+timeIntervalUnit));
//
//        // merge into the large table
//        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
//                        "t1.wikiproject AS wikiproject," +
//                        "t1.tcount AS tcount," +
//                        "t1.pre_tcount AS pre_tcount," +
//                        "t1.newbies_nbr AS newbies_nbr," +
//                        "t1.newcomers_nbr AS newcomers_nbr," +
//                        "t1.nbr_exp_newcomers AS nbr_exp_newcomers," +
//                        "t1.leavers_nbr AS leavers_nbr," +
//                        "t1.high_short_leavers_nbr AS high_short_leavers_nbr," +
//                        "t1.high_long_leavers_nbr AS high_long_leavers_nbr," +
//                        "t1.low_short_leavers_nbr AS low_short_leavers_nbr," +
//                        "t1.low_long_leavers_nbr AS low_long_leavers_nbr," +
//                        "t1.remainings_nbr AS remainings_nbr," +
//                        "t1.remainings_prod0 AS remainings_prod0," +
//                        "t1.remainings_coors45 AS remainings_coors45," +
//                        "t1.remainings_art_comm1 AS remainings_art_comm1," +
//                        "t1.remainings_user_comm3 AS remainings_user_comm3," +
//                        "t1.superficial_leavers_cnt AS superficial_leavers_cnt," +
//                        "t1.substantive_leavers_cnt AS substantive_leavers_cnt," +
//                        "t1.wp_tenure AS wp_tenure," +
//                        "t1.avg_substantive_leaver_tenure AS avg_substantive_leaver_tenure," +
//                        "t1.avg_last_tcount_ns0_edits AS avg_last_tcount_ns0_edits," +
//                        "t1.avg_last_tcount_ns45_edits AS avg_last_tcount_ns45_edits," +
//                        "t1.project_prod0 AS project_prod0," +
//                        "t1.project_coor4 AS project_coor4," +
//                        "t1.project_coor5 AS project_coor5," +
//                        "t1.project_coors45 AS project_coors45," +
//                        "t1.project_art_comm1 AS project_art_comm1," +
//                        "t1.project_user_comm3 AS project_user_comm3," +
//                        "t1.pre_project_prod0 AS pre_project_prod0," +
//                        "t1.pre_project_coors45 AS pre_project_coors45," +
//                        "t1.delta_prod0 AS delta_prod0," +
//                        "t1.delta_coors45 AS delta_coors45," +
//                        "t2.creation_tcount AS creation_tcount," +
//                        "FROM " + util.tableName(defaultDataset, "script_results_wp_ivs_cvs_group_abv0_dltDVs_all_newcomers_leavers"+timeIntervalUnit+"edit"+monthlyEditingThreshold, "t1") +
//                        "LEFT JOIN " + util.tableName(defaultDataset, "validproject_creation_date_tcount"+timeIntervalUnit, "t2") +
//                        "ON t1.nwikiproject = t2.nwikiproject " +
//                        "ORDER BY nwikiproject, tcount",
//                TableId.of(defaultDataset, "script_results_final"+timeIntervalUnit+"edit"+monthlyEditingThreshold));

        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.wikiproject AS wikiproject," +
                        "t1.tcount AS tcount," +
                        "t1.pre_tcount AS pre_tcount," +
                        "t1.newbies_nbr AS newbies_nbr," +
                        "t1.newcomers_nbr AS newcomers_nbr," +
                        "t1.nbr_committed_newcomers AS nbr_committed_newcomers," +
                        "t1.nbr_committed_leavers AS nbr_committed_leavers," +
                        "t1.nbr_prod_newcomers AS nbr_high_prod_newcomers," +
                        "t1.nbr_low_newcomers AS nbr_low_prod_newcomers," +
                        "t1.leavers_nbr AS leavers_nbr," +
                        "t1.high_short_leavers_nbr AS high_short_leavers_nbr," +
                        "t1.high_long_leavers_nbr AS high_long_leavers_nbr," +
                        "t1.low_short_leavers_nbr AS low_short_leavers_nbr," +
                        "t1.low_long_leavers_nbr AS low_long_leavers_nbr," +
                        "t1.remainings_nbr AS remainings_nbr," +
                        "t1.remainings_prod0 AS remainings_prod0," +
                        "t1.remainings_coors45 AS remainings_coors45," +
                        "t1.remainings_art_comm1 AS remainings_art_comm1," +
                        "t1.remainings_user_comm3 AS remainings_user_comm3," +
                        "t1.superficial_leavers_cnt AS superficial_leavers_cnt," +
                        "t1.substantive_leavers_cnt AS substantive_leavers_cnt," +
                        "t1.wp_tenure AS wp_tenure," +
                        "t1.avg_substantive_leaver_tenure AS avg_substantive_leaver_tenure," +
                        "t1.avg_last_tcount_ns0_edits AS avg_last_tcount_ns0_edits," +
                        "t1.avg_last_tcount_ns45_edits AS avg_last_tcount_ns45_edits," +
                        "t1.project_prod0 AS project_prod0," +
                        "t1.project_coor4 AS project_coor4," +
                        "t1.project_coor5 AS project_coor5," +
                        "t1.project_coors45 AS project_coors45," +
                        "t1.project_art_comm1 AS project_art_comm1," +
                        "t1.project_user_comm3 AS project_user_comm3," +
                        "t1.pre_project_prod0 AS pre_project_prod0," +
                        "t1.pre_project_coors45 AS pre_project_coors45," +
                        "t1.delta_prod0 AS delta_prod0," +
                        "t1.delta_coors45 AS delta_coors45," +
                        "t2.creation_tcount AS creation_tcount," +
                        "FROM " + util.tableName(defaultDataset, "script_results_wp_ivs_cvs_group_abv0_dltDVs_committed_prod_newcomers_leavers"+timeIntervalUnit+"edit"+monthlyEditingThreshold, "t1") +
                        "INNER JOIN " + util.tableName(defaultDataset, "validproject_creation_date_tcount"+timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject " +
                        "ORDER BY nwikiproject, tcount",
                TableId.of(defaultDataset, "script_results_final"+timeIntervalUnit+"edit"+monthlyEditingThreshold));



    }

    private void analysisOnEditorActivityAfterJoining() throws Exception {

        /**
         * SELECT t1.user_id AS user_id,
         t1.nwikiproject AS nwikiproject,
         t2.count AS time_interval,
         COUNT(*) AS productivity,
         FROM [bowen_user_dropouts.rev_ns0_valid_user_wikiproject] t1
         INNER JOIN [bowen_user_dropouts.lng_rev_intervals_0145_3months] t2
         ON t1.user_id = t2.user_id AND t1.nwikiproject = t2.nwikiproject
         WHERE t1.timestamp > t2.start_ts AND t1.timestamp < t2.end_ts
         GROUP BY user_id, nwikiproject, time_interval
         ORDER BY user_id, nwikiproject, time_interval

         */

        //
        // remove other wikiprojects

        // valid projects: bowen_quitting_script.wp_valid_tcounts
        // bowen_user_dropouts.lng_rev_intervals_0145_3months

        util.runQuery("SElECT t1.user_id AS user_id," +
                        "t1.nwikiproject AS nwikiproject," +
                        "t1.time_interval AS tcount," +
                        "t1.productivity AS productivity," +
                        "FROM " + util.tableName("bowen_user_dropouts", "lng_revs_amount_productivity_3months_intervals_seg", "t1") +
                        "INNER JOIN " + util.tableName("bowen_quitting_script", "wp_valid_tcounts", "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject",
                TableId.of(defaultDataset, "script_lng_revs_amount_productivity_3months_intervals_seg"));
    }

    private void allWikipediaUserSurvivialAnalysis() throws Exception {
//        SELECT rev_user_id,
//        MAX( rev_timestamp ) AS last_edit,
//        MIN(rev_timestamp) AS first_edit,
//        FROM [robert-kraut-1234:bowen_user_dropouts.revs]
//        GROUP BY rev_user_id

//        util.runQuery("SELECT rev_user_id AS user_id," +
//                        "MAX(rev_timestamp) AS last_edit_ts," +
//                        "MIN(rev_timestamp) AS first_edit_ts," +
////                        "(last_edit_ts - first_edit_ts) / (3600*24) AS staying_in_days)," +
//                        "FROM " + util.tableName("bowen_user_dropouts", "revs") +
//                        "WHERE PARSE_IP(rev_user_text) IS NULL " +
//                        "GROUP BY user_id ",
//                TableId.of(defaultDataset, "all_user_last_first_edits"));

//        util.runQuery("SELECT user_id," +
//                        "(last_edit_ts - first_edit_ts) / (3600*24) AS staying_in_days," +
//                        "FROM " + util.tableName(defaultDataset, "all_user_last_first_edits"),
//                TableId.of(defaultDataset, "all_user_last_first_edits_in_days"));
//
//        util.runQuery("SELECT COUNT(UNIQUE(user_id))," +
//                        "FROM " + util.tableName(defaultDataset, "all_user_last_first_edits"),
//                TableId.of(defaultDataset, "all_users_number"));


//        util.runQuery("SELECT COUNT(UNIQUE(user_id))," +
//                        "FROM " + util.tableName(defaultDataset, "all_user_last_first_edits_in_days") +
//                        "WHERE staying_in_days < 7",
//                TableId.of(defaultDataset, "all_users_number7"));
//
//        util.runQuery("SELECT COUNT(UNIQUE(user_id))," +
//                        "FROM " + util.tableName(defaultDataset, "all_user_last_first_edits_in_days") +
//                        "WHERE staying_in_days < 30",
//                TableId.of(defaultDataset, "all_users_number30"));

        // find a random day for tht week (time window)
        // 2011-02-20: 1298160000 +
        // 2008-12-01: 1228089600
        util.runQuery("SELECT COUNT(UNIQUE(user_id)) AS total_new_users," +
                        "FROM " + util.tableName(defaultDataset, "all_user_last_first_edits") +
                        "WHERE first_edit_ts >= 1228089600 AND first_edit_ts < 1228089600 + 3600*24*30",
                TableId.of(defaultDataset, "total_new_users_per_week"));

        util.runQuery("SELECT COUNT(UNIQUE(user_id)) AS total_leaving_users," +
                        "FROM " + util.tableName(defaultDataset, "all_user_last_first_edits") +
                        "WHERE last_edit_ts >= 1228089600 AND last_edit_ts < 1228089600 + 3600*24*30",
                TableId.of(defaultDataset, "total_leaving_users_per_week"));

        // newcomers: 62763; leavers: 64012

        // compute all the newcomers,

        // compute leavers



    }

    /**
     *  Create the newbies of wikiprojects
     */
    private void identityWikiProjectNewbies() throws Exception {
        // for all the newcomers to the project find when is their first edits in wikipedia..

        // identify users and their project by their ids
        util.runQuery("SElECT user_id," +
                        "nwikiproject," +
                        "MIN(timestamp) AS first_edit," +
                        "FROM " + util.tableName("bowen_wikis_quitting", "lng_user_wikiproject_valid_revs_45") +
                        "GROUP BY user_id, nwikiproject",
                TableId.of(defaultDataset, "script_wp_editor_first_edits"));


        // select only newcomers
        util.runQuery("SELECT t1.user_id AS user_id," +
                        "t1.nwikiproject AS nwikiproject," +
                        "t1.first_edit AS first_edit," +
                        "t2.tcount AS tcount," +
                        "FROM " + util.tableName(defaultDataset, "script_wp_editor_first_edits", "t1") +
                        "INNER JOIN " + util.tableName(defaultDataset, "script_wp_newcomers_tcount" + timeIntervalUnit + "edit" + monthlyEditingThreshold, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.user_id = t2.user_id",
                TableId.of(defaultDataset, "script_wp_newcomer_name_id" + timeIntervalUnit+"edit"+monthlyEditingThreshold));

        // select only newcomers without wikiprojects
        util.runQuery("SELECT user_id," +
                "COUNT(*) AS sum," +
                "FROM " + util.tableName(defaultDataset, "script_wp_newcomer_name_id" + timeIntervalUnit+"edit"+monthlyEditingThreshold) +
                "GROUP BY user_id",
                TableId.of(defaultDataset, "script_newcomer_ids_only"));

        // identify their first edits in wikipedia
        util.runQuery("SELECT t1.rev_user_id AS user_id," +
                        "MIN(t1.rev_timestamp) AS wp_first_edit," +
                        "FROM " + util.tableName("bowen_user_dropouts", "revs", "t1") +
                        "INNER JOIN " + util.tableName(defaultDataset, "script_newcomer_ids_only", "t2") +
                        "ON t1.rev_user_id = t2.user_id " +
                        "GROUP BY user_id",
                TableId.of(defaultDataset, "script_newcomer_first_edit_wp"));

        // join users with their project time range
        util.runQuery("SELECT t1.user_id AS user_id," +
                        "t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "t2.wp_first_edit AS wp_first_edit," +
                        "FROM " + util.tableName(defaultDataset, "script_wp_newcomers_tcount" + timeIntervalUnit + "edit" + monthlyEditingThreshold, "t1") +
                        "INNER JOIN " + util.tableName(defaultDataset, "script_newcomer_first_edit_wp", "t2") +
                        "ON t1.user_id = t2.user_id",
                TableId.of(defaultDataset, "script_newcomer_wp_first_edit_project_tcount" + timeIntervalUnit + "edit" + monthlyEditingThreshold));


        // add the timestamp for the tcount of the project
        util.runQuery("SELECT t1.user_id AS user_id," +
                        "t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "t2.start_ts AS start_ts," +
                        "t2.end_ts AS end_ts," +
                        "t1.wp_first_edit AS wp_first_edit," +
                        "FROM " + util.tableName(defaultDataset, "script_newcomer_wp_first_edit_project_tcount" + timeIntervalUnit + "edit" + monthlyEditingThreshold, "t1") +
                        "INNER JOIN " + util.tableName(defaultDataset, "script_user_wp_revs_45_valid_users_wps_valid_range"+timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount",
                TableId.of(defaultDataset, "script_newcomer_wp_first_edit_project_tcount_ts_range" + timeIntervalUnit + "edit" + monthlyEditingThreshold));


        // identify wp newbies
        util.runQuery("SELECT nwikiproject," +
                        "tcount," +
                        "COUNT(*) AS newbie_nbr," +
                        "FROM " + util.tableName(defaultDataset, "script_newcomer_wp_first_edit_project_tcount_ts_range" + timeIntervalUnit + "edit" + monthlyEditingThreshold) +
                        "WHERE wp_first_edit >= start_ts AND wp_first_edit < end_ts " +
                        "GROUP BY nwikiproject, tcount",
                TableId.of(defaultDataset, "script_wp_newbie_nbr_tcount" + timeIntervalUnit + "edit" + monthlyEditingThreshold));


        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "t1.newcomers_nbr AS newcomers_nbr," +
                        "IFNULL(t2.newbie_nbr, 0) AS newbies_nbr," +
                        "FROM " + util.tableName(defaultDataset, "script_wp_newcomers_nbr_tcount_full"+timeIntervalUnit+"edit"+monthlyEditingThreshold, "t1") +
                        "LEFT JOIN " + util.tableName(defaultDataset, "script_wp_newbie_nbr_tcount" + timeIntervalUnit + "edit" + monthlyEditingThreshold, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount",
                TableId.of(defaultDataset, "script_wp_newbie_nbr_tcount_full" + timeIntervalUnit + "edit" + monthlyEditingThreshold));

        // merge into result table
        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.wikiproject AS wikiproject," +
                        "t1.tcount AS tcount," +
                        "t1.pre_tcount AS pre_tcount," +
                        "IFNULL(t2.newbies_nbr, 0) AS newbies_nbr," +
                        "t1.newcomers_nbr AS newcomers_nbr," +
                        "t1.nbr_exp_newcomers AS nbr_exp_newcomers," +
                        "t1.leavers_nbr AS leavers_nbr," +
                        "t1.high_short_leavers_nbr AS high_short_leavers_nbr," +
                        "t1.high_long_leavers_nbr AS high_long_leavers_nbr," +
                        "t1.low_short_leavers_nbr AS low_short_leavers_nbr," +
                        "t1.low_long_leavers_nbr AS low_long_leavers_nbr," +
                        "t1.remainings_nbr AS remainings_nbr," +
                        "t1.remainings_prod0 AS remainings_prod0," +
                        "t1.remainings_coors45 AS remainings_coors45," +
                        "t1.remainings_art_comm1 AS remainings_art_comm1," +
                        "t1.remainings_user_comm3 AS remainings_user_comm3," +
                        "t1.superficial_leavers_cnt AS superficial_leavers_cnt," +
                        "t1.substantive_leavers_cnt AS substantive_leavers_cnt," +
                        "t1.wp_tenure AS wp_tenure," +
                        "t1.avg_substantive_leaver_tenure AS avg_substantive_leaver_tenure," +
                        "t1.avg_last_tcount_ns0_edits AS avg_last_tcount_ns0_edits," +
                        "t1.avg_last_tcount_ns45_edits AS avg_last_tcount_ns45_edits," +
                        "t1.project_prod0 AS project_prod0," +
                        "t1.project_coor4 AS project_coor4," +
                        "t1.project_coor5 AS project_coor5," +
                        "t1.project_coors45 AS project_coors45," +
                        "t1.project_art_comm1 AS project_art_comm1," +
                        "t1.project_user_comm3 AS project_user_comm3," +
                        "t1.pre_project_prod0 AS pre_project_prod0," +
                        "t1.pre_project_coors45 AS pre_project_coors45," +
                        "t1.delta_prod0 AS delta_prod0," +
                        "t1.delta_coors45 AS delta_coors45," +
                        "FROM " + util.tableName(defaultDataset, "script_results_wp_ivs_cvs_group_abv0_dltDVs_newcomers_leavers"+timeIntervalUnit+"edit"+monthlyEditingThreshold, "t1") +
                        "LEFT JOIN " + util.tableName(defaultDataset, "script_wp_newbie_nbr_tcount_full"+timeIntervalUnit+"edit"+monthlyEditingThreshold, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount " +
                        "ORDER BY nwikiproject, tcount",
                TableId.of(defaultDataset, "script_results_wp_ivs_cvs_group_abv0_dltDVs_all_newcomers_leavers"+timeIntervalUnit+"edit"+monthlyEditingThreshold));

    }

    /**
     * Identity short term and long term leavers, and their editing comments on project page and project talk pages
     */
    private void identifyShortAndLongTermLeavers() throws Exception {

        // mark short term and long term leavers
        util.runQuery("SELECT user_id," +
                        "nwikiproject," +
                        "tcount," +
                        "last_tcount," +
                        "IF(mbrship_length == 1, true, false) AS is_short_term," +
                        "IF(mbrship_length > 1, true, false) AS is_long_term," +
                        "FROM " + util.tableName(defaultDataset, "script_leavers_wp_tcount_acc_ns045" + timeIntervalUnit + "edit" + monthlyEditingThreshold),
                TableId.of(defaultDataset, "script_leavers_wp_tcount_acc_ns045_mbrship_length" + timeIntervalUnit + "edit" + monthlyEditingThreshold));

        // connect userid with users' edits to a specific project
        // Edits timestamp on ns45 of short term leavers
        util.runQuery("SELECT t1.user_id AS user_id," +
                        "t2.user_text AS user_text," +
                        "t1.nwikiproject AS nwikiproject," +
                        "t2.wikiproject AS wikiproject," +
                        "t2.timestamp AS timestamp," +
                        "t2.ns AS ns," +
                        "FROM " + util.tableName(defaultDataset, "script_leavers_wp_tcount_acc_ns045_mbrship_length" + timeIntervalUnit + "edit" + monthlyEditingThreshold, "t1") +
                        "INNER JOIN " + util.tableName("bowen_wikis_quitting", "lng_user_wikiproject_valid_revs_45", "t2") +
                        "ON t1.user_id = t2.user_id AND t1.nwikiproject = t2.nwikiproject " +
                        "WHERE t1.is_short_term IS TRUE",
                TableId.of(defaultDataset, "script_short_term_leavers_wp_edits_ts" + timeIntervalUnit + "edit" + monthlyEditingThreshold));


        // Edits timestamp on ns45 of long term leavers
        util.runQuery("SELECT t1.user_id AS user_id," +
                        "t2.user_text AS user_text," +
                        "t1.nwikiproject AS nwikiproject," +
                        "t2.wikiproject AS wikiproject," +
                        "t2.timestamp AS timestamp," +
                        "t2.ns AS ns," +
                        "FROM " + util.tableName(defaultDataset, "script_leavers_wp_tcount_acc_ns045_mbrship_length" + timeIntervalUnit + "edit" + monthlyEditingThreshold, "t1") +
                        "INNER JOIN " + util.tableName("bowen_wikis_quitting", "lng_user_wikiproject_valid_revs_45", "t2") +
                        "ON t1.user_id = t2.user_id AND t1.nwikiproject = t2.nwikiproject " +
                        "WHERE t1.is_long_term IS TRUE",
                TableId.of(defaultDataset, "script_long_term_leavers_wp_edits_ts" + timeIntervalUnit + "edit" + monthlyEditingThreshold));

        // identify the time range of the last tcounts of each wikiproject
        util.runQuery("SELECT t1.user_id AS user_id," +
                        "t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "t2.start_ts AS start_ts," +
                        "t2.end_ts AS end_ts," +
                        "FROM " + util.tableName(defaultDataset, "script_leavers_wp_tcount_acc_ns045_mbrship_length" + timeIntervalUnit + "edit" + monthlyEditingThreshold, "t1") +
                        "INNER JOIN " + util.tableName(defaultDataset, "script_user_wp_revs_45_valid_users_wps_valid_range" + timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount " +
                        "WHERE t1.tcount = t1.last_tcount AND t1.is_long_term IS TRUE",
                TableId.of(defaultDataset, "script_long_term_leavers_wp_last_tcounts" + timeIntervalUnit + "edit" + monthlyEditingThreshold));

        // identify the edits of long term editors in their last tcounts
                util.runQuery("SELECT t1.user_id AS user_id," +
                        "t2.user_text AS user_text," +
                        "t1.nwikiproject AS nwikiproject," +
                        "t2.wikiproject AS wikiproject," +
                        "t2.timestamp AS timestamp," +
                        "t2.ns AS ns," +
                        "FROM " + util.tableName(defaultDataset, "script_long_term_leavers_wp_last_tcounts" + timeIntervalUnit + "edit" + monthlyEditingThreshold, "t1") +
                        "INNER JOIN " + util.tableName("bowen_wikis_quitting", "lng_user_wikiproject_valid_revs_45", "t2") +
                        "ON t1.user_id = t2.user_id AND t1.nwikiproject = t2.nwikiproject " +
                        "WHERE t2.timestamp >= t1.start_ts AND t2.timestamp < t1.end_ts",
                TableId.of(defaultDataset, "script_long_term_leavers_wp_edits_ts_last_tcounts" + timeIntervalUnit + "edit" + monthlyEditingThreshold));

        // Find the comments of those editors in the revision for short terms leavers
        util.runQuery("SELECT t1.user_text AS user_text," +
                        "t1.wikiproject AS wikiproject," +
                        "t1.timestamp AS timestamp," +
                        "t1.ns AS ns," +
                        "t2.rev_comment AS rev_comment," +
                        "FROM " + util.tableName(defaultDataset, "script_short_term_leavers_wp_edits_ts" + timeIntervalUnit + "edit" + monthlyEditingThreshold, "t1") +
                        "INNER JOIN " + util.tableName("bowen_user_dropouts", "revs", "t2") +
                        "ON t1.user_text = t2.rev_user_text AND t1.timestamp = t2.rev_timestamp " +
                        "WHERE (t2.ns = 4 OR t2.ns = 5) AND t2.rev_comment IS NOT NULL",
                TableId.of(defaultDataset, "script_short_term_leavers_wp_edits_comments" + timeIntervalUnit + "edit" + monthlyEditingThreshold));

        // Find the comments of those editors in the revision for long term leavers
        util.runQuery("SELECT t1.user_text AS user_text," +
                        "t1.wikiproject AS wikiproject," +
                        "t1.timestamp AS timestamp," +
                        "t1.ns AS ns," +
                        "t2.rev_comment AS rev_comment," +
                        "FROM " + util.tableName(defaultDataset, "script_long_term_leavers_wp_edits_ts" + timeIntervalUnit + "edit" + monthlyEditingThreshold, "t1") +
                        "INNER JOIN " + util.tableName("bowen_user_dropouts", "revs", "t2") +
                        "ON t1.user_text = t2.rev_user_text AND t1.timestamp = t2.rev_timestamp " +
                        "WHERE (t2.ns = 4 OR t2.ns = 5) AND t2.rev_comment IS NOT NULL",
                TableId.of(defaultDataset, "script_longer_term_leavers_wp_edits_comments" + timeIntervalUnit + "edit" + monthlyEditingThreshold));

        // Find the comments of those editors in the revision for long term leavers of edits in their last tcounts
        util.runQuery("SELECT t1.user_text AS user_text," +
                        "t1.wikiproject AS wikiproject," +
                        "t1.timestamp AS timestamp," +
                        "t1.ns AS ns," +
                        "t2.rev_comment AS rev_comment," +
                        "FROM " + util.tableName(defaultDataset, "script_long_term_leavers_wp_edits_ts_last_tcounts" + timeIntervalUnit + "edit" + monthlyEditingThreshold, "t1") +
                        "INNER JOIN " + util.tableName("bowen_user_dropouts", "revs", "t2") +
                        "ON t1.user_text = t2.rev_user_text AND t1.timestamp = t2.rev_timestamp " +
                        "WHERE (t2.ns = 4 OR t2.ns = 5) AND t2.rev_comment IS NOT NULL",
                TableId.of(defaultDataset, "script_longer_term_leavers_wp_edits_comments_last_tcounts" + timeIntervalUnit + "edit" + monthlyEditingThreshold));

        // select only the comments and for long term editors, picking 65,596 which is the number of short term leavers?
        util.runQuery("SELECT rev_comment," +
                        "FROM " + util.tableName(defaultDataset, "script_short_term_leavers_wp_edits_comments" + timeIntervalUnit + "edit" + monthlyEditingThreshold),
                TableId.of(defaultDataset, "script_short_term_leavers_wp_edits_comments_only" + timeIntervalUnit + "edit" + monthlyEditingThreshold));

        util.runQuery("SELECT rev_comment," +
                        "FROM " + util.tableName(defaultDataset, "script_longer_term_leavers_wp_edits_comments" + timeIntervalUnit + "edit" + monthlyEditingThreshold) +
                        "LIMIT 65596",
                TableId.of(defaultDataset, "script_long_term_leavers_wp_edits_comments_only" + timeIntervalUnit + "edit" + monthlyEditingThreshold));

        util.runQuery("SELECT rev_comment," +
                        "FROM " + util.tableName(defaultDataset, "script_longer_term_leavers_wp_edits_comments_last_tcounts" + timeIntervalUnit + "edit" + monthlyEditingThreshold) +
                        "LIMIT 65596",
                TableId.of(defaultDataset, "script_long_term_leavers_wp_edits_comments_last_tcounts_only" + timeIntervalUnit + "edit" + monthlyEditingThreshold));

        // create data for human coding
        util.runQuery("SELECT user_text," +
                        "wikiproject," +
                        "timestamp," +
                        "rev_comment," +
                        "RAND(100) AS rand," +
                        "FROM " + util.tableName(defaultDataset, "script_longer_term_leavers_wp_edits_comments_last_tcounts" + timeIntervalUnit + "edit" + monthlyEditingThreshold) +
                        "ORDER BY rand " +
                        "LIMIT 100",
                TableId.of(defaultDataset, "script_longer_term_leavers_wp_edits_comments_last_tcounts_sampled" + timeIntervalUnit + "edit" + monthlyEditingThreshold));

        util.runQuery("SELECT user_text," +
                        "wikiproject," +
                        "timestamp," +
                        "rev_comment," +
                        "RAND(100) AS rand," +
                        "FROM " + util.tableName(defaultDataset, "script_short_term_leavers_wp_edits_comments" + timeIntervalUnit + "edit" + monthlyEditingThreshold) +
                        "ORDER BY rand " +
                        "LIMIT 100",
                TableId.of(defaultDataset, "script_short_term_leavers_wp_edits_comments_sampled" + timeIntervalUnit + "edit" + monthlyEditingThreshold));
    }


    /**
     * the accumulated amounts of edits made on the wikipedia before the newcomers joined
     */
    private void addNewcomerAccumulatedWikipediaEditsInEachTcountWithinWPToResults() throws Exception {
        // sum up the total amount of edits in all the four categories (ns 0, 1, 4, 5)
//        util.runQuery("SELECT rev_user_text AS user_text," +
//                        "rev_user_id AS user_id," +
//                        "ns AS ns," +
//                        "rev_timestamp AS timestamp," +
//                        "FROM " + util.tableName("bowen_user_dropouts", "revs") +
//                        "WHERE ns = 0 OR ns = 1 OR ns = 4 OR ns = 5",
//                TableId.of(defaultDataset, "script_user_edits_ns0145"));

        // TODO: this should be simplied by calculating edits in each namespace and add them up
        // select newcomers, and the time of each wikiproject tcounts
        // edits on the articles within the scope of the project (ns0) - # of edits before joining the current tcount (include the edits before joining the project)
//        util.runQuery("SELECT t1.user_id AS user_id," +
//                        "t1.nwikiproject AS nwikiproject," +
//                        "t1.tcount AS tcount," +
//                        "t1.first_tcount AS first_tcount," +
//                        "t1.last_tcount AS last_tcount," +
//                        "t1.start_ts AS start_ts," +
//                        "t1.end_ts AS end_ts," +
//                        "t2.timestamp AS timestamp," +
//                        "FROM " + util.tableName(defaultDataset, "script_editor_project_active_range_ts" + timeIntervalUnit + "edit" + monthlyEditingThreshold, "t1") +
//                        "INNER JOIN " + util.tableName(defaultDataset, "script_user_edits_ns0145", "t2") +
//                        "ON t1.user_id = t2.user_id " +
////                        "WHERE t1.start_ts <= t2.timestamp AND t1.end_ts > timestamp", // range of edits & need to include newcomers
//                        "WHERE t1.end_ts > t2.timestamp", // range of edits & need to include newcomers
//                TableId.of(defaultDataset, "script_editor_project_active_range_edits_ns0145" + timeIntervalUnit + "edit" + monthlyEditingThreshold));
//
//

//        // aggregated edits of remaining members
//        util.runQuery("SELECT user_id," +
//                        "nwikiproject," +
//                        "tcount," +
//                        "COUNT(*) AS acc_edits0145," +
//                        "FROM " + util.tableName(defaultDataset, "script_editor_project_active_range_edits_ns0145" + timeIntervalUnit + "edit" + monthlyEditingThreshold) +
//                        "WHERE tcount != first_tcount " +
//                        "GROUP BY user_id, nwikiproject, tcount",
//                TableId.of(defaultDataset, "script_editor_remainings_project_active_range_acc_edits_ns0145" + timeIntervalUnit + "edit" + monthlyEditingThreshold));
//
//        // median value of remaining members for each project
//        util.runQuery("SELECT nwikiproject," +
//                        "tcount," +
//                        "NTH(501, QUANTILES(acc_edits0145, 1001)) AS median_acc_ns0145," + // median value to the lower side
//                        "FROM " + util.tableName(defaultDataset, "script_editor_remainings_project_active_range_acc_edits_ns0145" + timeIntervalUnit + "edit" + monthlyEditingThreshold) +
//                        "GROUP BY nwikiproject, tcount",
//                TableId.of(defaultDataset, "script_wp_full_editor_acc_ns0145_median" + timeIntervalUnit + "edit" + monthlyEditingThreshold));
//
//
//        // aggregated edits of newcomers
//        util.runQuery("SELECT user_id," +
//                        "nwikiproject," +
//                        "tcount," +
//                        "COUNT(*) AS acc_edits0145," +
//                        "FROM " + util.tableName(defaultDataset, "script_editor_project_active_range_edits_ns0145" + timeIntervalUnit + "edit" + monthlyEditingThreshold) +
//                        "WHERE tcount = first_tcount " +
//                        "GROUP BY user_id, nwikiproject, tcount",
//                TableId.of(defaultDataset, "script_editor_newcomers_project_active_range_acc_edits_ns0145" + timeIntervalUnit + "edit" + monthlyEditingThreshold));

//        // identify newcomers first tcounts
//        util.runQuery("SELECT t1.user_id AS user_id," +
//                        "t1.nwikiproject AS nwikiproject," +
//                        "t1.tcount AS tcount," +
//                        "t1.acc_edits0145 AS acc_ns0145," +
//                        "FROM " + util.tableName(defaultDataset, "script_editor_newcomers_project_active_range_acc_edits_ns0145" + timeIntervalUnit + "edit" + monthlyEditingThreshold, "t1") +
//                        "INNER JOIN " + util.tableName(defaultDataset, "script_editor_project_active_range_ts" + timeIntervalUnit + "edit" + monthlyEditingThreshold, "t2") +
//                        "ON t1.user_id = t2.user_id AND t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount ",
//                TableId.of(defaultDataset, "script_newcomers_wp_tcount_acc_ns0145" + timeIntervalUnit + "edit" + monthlyEditingThreshold));
//
//        // mark productive/unproductive newcomers
//        util.runQuery("SELECT t1.user_id AS user_id," +
//                        "t1.nwikiproject AS nwikiproject," +
//                        "t1.tcount AS tcount," +
//                        "IF(t1.acc_ns0145 > t2.median_acc_ns0145, 1, 0) AS is_high_prod_newcomers," +
//                        "IF(t1.acc_ns0145 <= t2.median_acc_ns0145, 1, 0) AS is_low_prod_newcomers," +
//                        "FROM " + util.tableName(defaultDataset, "script_newcomers_wp_tcount_acc_ns0145" + timeIntervalUnit + "edit" + monthlyEditingThreshold, "t1") +
//                        "INNER JOIN " + util.tableName(defaultDataset, "script_wp_full_editor_acc_ns0145_median" + timeIntervalUnit + "edit" + monthlyEditingThreshold, "t2") +
//                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount",
//                TableId.of(defaultDataset, "script_newcomers_wp_tcount_acc_ns0145_efficiency" + timeIntervalUnit + "edit" + monthlyEditingThreshold));
//
//        // sum up four types of leavers - missing tcounts are those have no leavers
//        util.runQuery("SELECT nwikiproject," +
//                        "tcount," +
//                        "SUM(is_high_prod_newcomers) AS high_prod_newcomers_nbr," +
//                        "SUM(is_low_prod_newcomers) AS low_prod_newcomers_nbr," +
//                        "FROM " + util.tableName(defaultDataset, "script_newcomers_wp_tcount_acc_ns0145_efficiency" + timeIntervalUnit + "edit" + monthlyEditingThreshold) +
//                        "GROUP BY nwikiproject, tcount",
//                TableId.of(defaultDataset, "script_wp_tcount_acc_ns0145_newcomers_prod" + timeIntervalUnit + "edit" + monthlyEditingThreshold));


        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.wikiproject AS wikiproject," +
                        "t1.tcount AS tcount," +
                        "t1.pre_tcount AS pre_tcount," +
                        "t1.newbies_nbr AS newbies_nbr," +
                        "t1.newcomers_nbr AS newcomers_nbr," +
                        "t1.nbr_committed_newcomers AS nbr_committed_newcomers," +
                        "t1.nbr_committed_leavers AS nbr_committed_leavers," +
                        "IFNULL(t2.high_prod_newcomers_nbr, 0) nbr_prod_newcomers," +
                        "IFNULL(t2.low_prod_newcomers_nbr, 0) nbr_low_newcomers," +
                        "t1.leavers_nbr AS leavers_nbr," +
                        "t1.high_short_leavers_nbr AS high_short_leavers_nbr," +
                        "t1.high_long_leavers_nbr AS high_long_leavers_nbr," +
                        "t1.low_short_leavers_nbr AS low_short_leavers_nbr," +
                        "t1.low_long_leavers_nbr AS low_long_leavers_nbr," +
                        "t1.remainings_nbr AS remainings_nbr," +
                        "t1.remainings_prod0 AS remainings_prod0," +
                        "t1.remainings_coors45 AS remainings_coors45," +
                        "t1.remainings_art_comm1 AS remainings_art_comm1," +
                        "t1.remainings_user_comm3 AS remainings_user_comm3," +
                        "t1.superficial_leavers_cnt AS superficial_leavers_cnt," +
                        "t1.substantive_leavers_cnt AS substantive_leavers_cnt," +
                        "t1.wp_tenure AS wp_tenure," +
                        "t1.avg_substantive_leaver_tenure AS avg_substantive_leaver_tenure," +
                        "t1.avg_last_tcount_ns0_edits AS avg_last_tcount_ns0_edits," +
                        "t1.avg_last_tcount_ns45_edits AS avg_last_tcount_ns45_edits," +
                        "t1.project_prod0 AS project_prod0," +
                        "t1.project_coor4 AS project_coor4," +
                        "t1.project_coor5 AS project_coor5," +
                        "t1.project_coors45 AS project_coors45," +
                        "t1.project_art_comm1 AS project_art_comm1," +
                        "t1.project_user_comm3 AS project_user_comm3," +
                        "t1.pre_project_prod0 AS pre_project_prod0," +
                        "t1.pre_project_coors45 AS pre_project_coors45," +
                        "t1.delta_prod0 AS delta_prod0," +
                        "t1.delta_coors45 AS delta_coors45," +
                        "FROM " + util.tableName(defaultDataset, "script_results_wp_ivs_cvs_group_abv0_dltDVs_committed_newcomers_leavers"+timeIntervalUnit+"edit"+monthlyEditingThreshold, "t1") +
                        "INNER JOIN " + util.tableName(defaultDataset, "script_wp_tcount_acc_ns0145_newcomers_prod"+timeIntervalUnit+"edit"+monthlyEditingThreshold, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount " +
                        "ORDER BY nwikiproject, tcount",
                TableId.of(defaultDataset, "script_results_wp_ivs_cvs_group_abv0_dltDVs_committed_prod_newcomers_leavers"+timeIntervalUnit+"edit"+monthlyEditingThreshold));

    }


    /**
     * the accumulated amount of edits the editor made on project articles and project page
     */
    private void addLeaversAccumulatedProjectEditsInEachTcountWithinWPToResults() throws Exception {

        // Editor project active range plus the timestamp range of the tcounts
        util.runQuery("SELECT t1.user_id AS user_id," +
                        "t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "t1.first_tcount AS first_tcount," +
                        "t1.last_tcount AS last_tcount," +
                        "t2.start_ts AS start_ts," +
                        "t2.end_ts AS end_ts," +
                        "FROM " + util.tableName(defaultDataset, "script_user_wp_active_range_revs45" + timeIntervalUnit + "edit" + monthlyEditingThreshold, "t1") +
                        "INNER JOIN " + util.tableName(defaultDataset, "script_user_wp_revs_45_valid_users_wps_valid_range" + timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount",
                TableId.of(defaultDataset, "script_editor_project_active_range_ts" + timeIntervalUnit + "edit" + monthlyEditingThreshold));


        // edits on the articles within the scope of the project (ns0) - # of edits before joining the current tcount (include the edits before joining the project)
        util.runQuery("SELECT t1.user_id AS user_id," +
                        "t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
//                "t1.first_tcount AS first_tcount," +
//                "t1.last_tcount AS last_tcount," +
                        "t1.start_ts AS start_ts," +
                        "t1.end_ts AS end_ts," +
                        "t2.timestamp AS timestamp," +
                        "FROM " + util.tableName(defaultDataset, "script_editor_project_active_range_ts" + timeIntervalUnit + "edit" + monthlyEditingThreshold, "t1") +
                        "INNER JOIN " + util.tableName("bowen_user_dropouts", "rev_ns0_valid_user_wikiproject_std", "t2") +
                        "ON t1.user_id = t2.user_id AND t1.nwikiproject = t2.nwikiproject " +
//                        "WHERE t1.start_ts <= t2.timestamp AND t1.end_ts > timestamp", // range of edits & need to include newcomers
                        "WHERE t1.end_ts > t2.timestamp", // range of edits & need to include newcomers
                TableId.of(defaultDataset, "script_editor_project_active_range_edits_ns0" + timeIntervalUnit + "edit" + monthlyEditingThreshold));

        util.runQuery("SELECT user_id," +
                        "nwikiproject," +
                        "tcount," +
                        "COUNT(*) AS acc_prod0," +
                        "FROM " + util.tableName(defaultDataset, "script_editor_project_active_range_edits_ns0" + timeIntervalUnit + "edit" + monthlyEditingThreshold) +
                        "GROUP BY user_id, nwikiproject, tcount",
                TableId.of(defaultDataset, "script_editor_project_active_range_acc_edits_ns0" + timeIntervalUnit + "edit" + monthlyEditingThreshold));

        // TODO: edits on the article talk pages within the scope of the project (ns1)

        // edits on the project page and project talk page
        util.runQuery("SELECT t1.user_id AS user_id," +
                        "t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "t1.start_ts AS start_ts," +
                        "t1.end_ts AS end_ts," +
                        "t2.timestamp AS timestamp," +
                        "FROM " + util.tableName(defaultDataset, "script_editor_project_active_range_ts" + timeIntervalUnit + "edit" + monthlyEditingThreshold, "t1") +
                        "INNER JOIN " + util.tableName("bowen_wikis_quitting", "rev_ns45_user_wikiproject_ts_encoded_full", "t2") +
                        "ON t1.user_id = t2.user_id AND t1.nwikiproject = t2.nwikiproject " +
//                        "WHERE t1.start_ts <= t2.timestamp AND t1.end_ts > timestamp", // need to include newcomers
                        "WHERE t1.end_ts > t2.timestamp", // range of edits & need to include newcomers
                TableId.of(defaultDataset, "script_editor_project_active_range_edits_ns45" + timeIntervalUnit + "edit" + monthlyEditingThreshold));

        util.runQuery("SELECT user_id," +
                        "nwikiproject," +
                        "tcount," +
                        "COUNT(*) AS acc_coors45," +
                        "FROM " + util.tableName(defaultDataset, "script_editor_project_active_range_edits_ns45" + timeIntervalUnit + "edit" + monthlyEditingThreshold) +
                        "GROUP BY user_id, nwikiproject, tcount",
                TableId.of(defaultDataset, "script_editor_project_active_range_acc_edits_ns45" + timeIntervalUnit + "edit" + monthlyEditingThreshold));


        // join with editor's full active range with editor accumulated productivity
        util.runQuery("SELECT t1.user_id AS user_id," +
                        "t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "IFNULL(t2.acc_prod0, 0) AS acc_prod0," +
                        "FROM " + util.tableName(defaultDataset, "script_user_wp_active_range_revs45" + timeIntervalUnit + "edit" + monthlyEditingThreshold, "t1") +
                        "LEFT JOIN " + util.tableName(defaultDataset, "script_editor_project_active_range_acc_edits_ns0" + timeIntervalUnit + "edit" + monthlyEditingThreshold, "t2") +
                        "ON t1.user_id = t2.user_id AND t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount ",
//                        "WHERE t1.tcount != first_tcount",
                TableId.of(defaultDataset, "script_editor_wp_tcount_acc_ns0" + timeIntervalUnit + "edit" + monthlyEditingThreshold));

        // merge with editor's accumulated coordination
        util.runQuery("SELECT t1.user_id AS user_id," +
                        "t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "IF(t2.acc_coors45 IS NULL, t1.acc_prod0, t1.acc_prod0+t2.acc_coors45) AS acc_ns045," +
                        "FROM " + util.tableName(defaultDataset, "script_editor_wp_tcount_acc_ns0" + timeIntervalUnit + "edit" + monthlyEditingThreshold, "t1") +
                        "LEFT JOIN " + util.tableName(defaultDataset, "script_editor_project_active_range_acc_edits_ns45" + timeIntervalUnit + "edit" + monthlyEditingThreshold, "t2") +
                        "ON t1.user_id = t2.user_id AND t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount",
                TableId.of(defaultDataset, "script_editor_wp_tcount_acc_ns045" + timeIntervalUnit + "edit" + monthlyEditingThreshold));

        // median value for each project
        util.runQuery("SELECT nwikiproject," +
                        "tcount," +
                        "NTH(501, QUANTILES(acc_ns045, 1001)) AS median_acc_ns045," + // median value to the lower side
                        "FROM " + util.tableName(defaultDataset, "script_editor_wp_tcount_acc_ns045" + timeIntervalUnit + "edit" + monthlyEditingThreshold) +
                        "GROUP BY nwikiproject, tcount",
                TableId.of(defaultDataset, "script_wp_full_editor_acc_ns045_median" + timeIntervalUnit + "edit" + monthlyEditingThreshold));


        // identify leavers last tcounts
        util.runQuery("SELECT t1.user_id AS user_id," +
                        "t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "t1.acc_ns045 AS acc_ns045," +
                        "t2.first_tcount AS first_tcount," +
                        "t2.last_tcount AS last_tcount," +
                        "(t2.last_tcount-t2.first_tcount+1) AS mbrship_length," +
                        "FROM " + util.tableName(defaultDataset, "script_editor_wp_tcount_acc_ns045" + timeIntervalUnit + "edit" + monthlyEditingThreshold, "t1") +
                        "INNER JOIN " + util.tableName(defaultDataset, "script_editor_project_active_range_ts" + timeIntervalUnit + "edit" + monthlyEditingThreshold, "t2") +
                        "ON t1.user_id = t2.user_id AND t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount " +
                        "WHERE t1.tcount = t2.last_tcount",
                TableId.of(defaultDataset, "script_leavers_wp_tcount_acc_ns045" + timeIntervalUnit + "edit" + monthlyEditingThreshold));

        // mark productive/unproductive leavers
        util.runQuery("SELECT t1.user_id AS user_id," +
                        "t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "(t1.tcount+1) AS leaving_tcount," +
                        "IF(t1.acc_ns045 > t2.median_acc_ns045, true, false) AS is_high_prod_leavers," +
                        "IF(t1.acc_ns045 <= t2.median_acc_ns045, true, false) AS is_low_prod_leavers," +
                        "FROM " + util.tableName(defaultDataset, "script_leavers_wp_tcount_acc_ns045" + timeIntervalUnit + "edit" + monthlyEditingThreshold, "t1") +
                        "INNER JOIN " + util.tableName(defaultDataset, "script_wp_full_editor_acc_ns045_median" + timeIntervalUnit + "edit" + monthlyEditingThreshold, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount",
                TableId.of(defaultDataset, "script_leavers_wp_tcount_acc_ns045_efficiency" + timeIntervalUnit + "edit" + monthlyEditingThreshold));

        // mark short term and long term leavers
        util.runQuery("SELECT user_id," +
                        "nwikiproject," +
                        "tcount," +
                        "IF(mbrship_length == 1, true, false) AS is_short_term," +
                        "IF(mbrship_length > 1, true, false) AS is_long_term," +
                        "FROM " + util.tableName(defaultDataset, "script_leavers_wp_tcount_acc_ns045" + timeIntervalUnit + "edit" + monthlyEditingThreshold),
                TableId.of(defaultDataset, "script_leavers_wp_tcount_acc_ns045_mbrship_length" + timeIntervalUnit + "edit" + monthlyEditingThreshold));

        // join two dimensions together
        util.runQuery("SELECT t1.user_id AS user_id," +
                        "t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "t1.leaving_tcount AS leaving_tcount," +
                        "t1.is_high_prod_leavers AS is_high_prod_leavers," +
                        "t1.is_low_prod_leavers AS is_low_prod_leavers," +
                        "t2.is_short_term AS is_short_term," +
                        "t2.is_long_term AS is_long_term," +
                        "IF (t1.is_high_prod_leavers AND t2.is_short_term, 1, 0) AS high_short," +
                        "IF (t1.is_high_prod_leavers AND t2.is_long_term, 1, 0) AS high_long," +
                        "IF (t1.is_low_prod_leavers AND t2.is_short_term, 1, 0) AS low_short," +
                        "IF (t1.is_low_prod_leavers AND t2.is_long_term, 1, 0) AS low_long," +
                        "FROM " + util.tableName(defaultDataset, "script_leavers_wp_tcount_acc_ns045_efficiency" + timeIntervalUnit + "edit" + monthlyEditingThreshold, "t1") +
                        "INNER JOIN " + util.tableName(defaultDataset, "script_leavers_wp_tcount_acc_ns045_mbrship_length" + timeIntervalUnit + "edit" + monthlyEditingThreshold, "t2") +
                        "ON t1.user_id = t2.user_id AND t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount",
                TableId.of(defaultDataset, "script_leavers_wp_tcount_acc_ns045_leavers_attrs" + timeIntervalUnit + "edit" + monthlyEditingThreshold));

        // sum up four types of leavers - missing tcounts are those have no leavers
        util.runQuery("SELECT nwikiproject," +
                        "leaving_tcount," +
                        "SUM(high_short) AS high_short_leavers_nbr," +
                        "SUM(high_long) AS high_long_leavers_nbr," +
                        "SUM(low_short) AS low_short_leavers_nbr," +
                        "SUM(low_long) AS low_long_leavers_nbr," +
                        "FROM " + util.tableName(defaultDataset, "script_leavers_wp_tcount_acc_ns045_leavers_attrs" + timeIntervalUnit + "edit" + monthlyEditingThreshold) +
                        "GROUP BY nwikiproject, leaving_tcount",
                TableId.of(defaultDataset, "script_wp_tcount_acc_ns45_leavers_attrs" + timeIntervalUnit + "edit" + monthlyEditingThreshold));


        // merge into result table
        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.wikiproject AS wikiproject," +
                        "t1.tcount AS tcount," +
                        "t1.pre_tcount AS pre_tcount," +
                        "t1.newcomers_nbr AS newcomers_nbr," +
                        "t1.nbr_exp_newcomers AS nbr_exp_newcomers," +
                        "t1.leavers_nbr AS leavers_nbr," +
                        "IFNULL(t2.high_short_leavers_nbr, 0) AS high_short_leavers_nbr," +
                        "IFNULL(t2.high_long_leavers_nbr, 0) AS high_long_leavers_nbr," +
                        "IFNULL(t2.low_short_leavers_nbr, 0) AS low_short_leavers_nbr," +
                        "IFNULL(t2.low_long_leavers_nbr, 0) AS low_long_leavers_nbr," +
                        "t1.remainings_nbr AS remainings_nbr," +
                        "t1.remainings_prod0 AS remainings_prod0," +
                        "t1.remainings_coors45 AS remainings_coors45," +
                        "t1.remainings_art_comm1 AS remainings_art_comm1," +
                        "t1.remainings_user_comm3 AS remainings_user_comm3," +
                        "t1.superficial_leavers_cnt AS superficial_leavers_cnt," +
                        "t1.substantive_leavers_cnt AS substantive_leavers_cnt," +
                        "t1.wp_tenure AS wp_tenure," +
                        "t1.avg_substantive_leaver_tenure AS avg_substantive_leaver_tenure," +
                        "t1.avg_last_tcount_ns0_edits AS avg_last_tcount_ns0_edits," +
                        "t1.avg_last_tcount_ns45_edits AS avg_last_tcount_ns45_edits," +
                        "t1.project_prod0 AS project_prod0," +
                        "t1.project_coor4 AS project_coor4," +
                        "t1.project_coor5 AS project_coor5," +
                        "t1.project_coors45 AS project_coors45," +
                        "t1.project_art_comm1 AS project_art_comm1," +
                        "t1.project_user_comm3 AS project_user_comm3," +
                        "t1.pre_project_prod0 AS pre_project_prod0," +
                        "t1.pre_project_coors45 AS pre_project_coors45," +
                        "t1.delta_prod0 AS delta_prod0," +
                        "t1.delta_coors45 AS delta_coors45," +
                        "FROM " + util.tableName(defaultDataset, "script_results_wp_ivs_cvs_group_abv0_dltDVs_newcomers"+timeIntervalUnit+"edit"+monthlyEditingThreshold, "t1") +
                        "LEFT JOIN " + util.tableName(defaultDataset, "script_wp_tcount_acc_ns45_leavers_attrs"+timeIntervalUnit+"edit"+monthlyEditingThreshold, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.leaving_tcount " +
                        "ORDER BY nwikiproject, tcount",
                TableId.of(defaultDataset, "script_results_wp_ivs_cvs_group_abv0_dltDVs_newcomers_leavers"+timeIntervalUnit+"edit"+monthlyEditingThreshold));
    }


    /**
     * # of wikiproject the newcomer joined before joining the current project
     */
    private void addNewcomerLeaverPriorExperienceToResults() throws Exception {


//        // compute for newcomers
//        // select the prior wikiprojects joined before joining the focal project
//        util.runQuery("SELECT t1.user_id AS user_id," +
//                        "t1.nwikiproject AS nwikiproject," +
//                        "t1.first_tcount AS focal_first_tcount," +
//                        "t2.nwikiproject AS prior_nwikiproject," +
//                        "FROM " + util.tableName(defaultDataset, "script_user_wp_active_range_revs45" + timeIntervalUnit + "edit" + monthlyEditingThreshold, "t1") +
//                        "INNER JOIN " + util.tableName(defaultDataset, "script_user_wp_active_range_revs45" + timeIntervalUnit + "edit" + monthlyEditingThreshold, "t2") +
//                        "ON t1.user_id = t2.user_id " +
//                        "WHERE t1.first_tcount > t2.tcount",
//                TableId.of(defaultDataset, "script_user_wp_prior_wps" + timeIntervalUnit + "edit" + monthlyEditingThreshold));
//
//        util.runQuery("SELECT user_id," +
//                        "nwikiproject," +
//                        "COUNT(*) AS prior_wp_cnt," +
//                        "FROM " + util.tableName(defaultDataset, "script_user_wp_prior_wps" + timeIntervalUnit + "edit" + monthlyEditingThreshold) +
//                        "GROUP BY user_id, nwikiproject " +
//                        "ORDER BY user_id, nwikiproject",
//                TableId.of(defaultDataset, "script_user_wp_prior_wps_cnt" + timeIntervalUnit + "edit" + monthlyEditingThreshold));
//
//        util.runQuery("SELECT t1.user_id AS user_id," +
//                        "t1.nwikiproject AS nwikiproject," +
////                        "IFNULL(t2.prior_wp_cnt, 0) AS prior_wp_cnt," +
//                        "IF(t2.user_id IS NULL AND t2.nwikiproject IS NULL, 0, t2.prior_wp_cnt) AS prior_wp_cnt," +
//                        "FROM " + util.tableName(defaultDataset, "script_user_wp_first_last_tcounts" + timeIntervalUnit + "edits" + monthlyEditingThreshold, "t1") +
//                        "LEFT JOIN " + util.tableName(defaultDataset, "script_user_wp_prior_wps_cnt" + timeIntervalUnit + "edit" + monthlyEditingThreshold, "t2") +
//                "ON t1.user_id = t2.user_id AND t1.nwikiproject = t2.nwikiproject",
//                TableId.of(defaultDataset, "script_user_wp_prior_wps_cnt_full" + timeIntervalUnit + "edit" + monthlyEditingThreshold));
//
//        // identify newcomers
//        util.runQuery("SELECT user_id," +
//                        "nwikiproject," +
//                        "tcount," +
//                        "FROM " + util.tableName(defaultDataset, "script_user_wp_active_range_revs45" + timeIntervalUnit + "edit" + monthlyEditingThreshold) +
//                        "WHERE tcount = first_tcount",
//                TableId.of(defaultDataset, "script_wp_newcomers_tcount" + timeIntervalUnit+"edit"+monthlyEditingThreshold));
//
//        // binarize the number of projects joined for the editor has or doesn't have prior experience for newcomers
//        util.runQuery("SELECT t1.user_id AS user_id," +
//                        "t1.nwikiproject AS nwikiproject," +
//                        "t1.tcount AS tcount," +
//                        "t2.prior_wp_cnt AS prior_wp_cnt," +
//                        "IF(t2.prior_wp_cnt > 0, 1, 0) AS has_prior_wp," +
//                        "FROM " + util.tableName(defaultDataset, "script_wp_newcomers_tcount" + timeIntervalUnit + "edit" + monthlyEditingThreshold, "t1") +
//                        "INNER JOIN " + util.tableName(defaultDataset, "script_user_wp_prior_wps_cnt_full" + timeIntervalUnit + "edit" + monthlyEditingThreshold, "t2") +
//                        "ON t1.user_id = t2.user_id AND t1.nwikiproject = t2.nwikiproject",
//                TableId.of(defaultDataset, "script_newcomer_prior_wp_experience" + timeIntervalUnit + "edit" + monthlyEditingThreshold));
//
//        // # of experienced newcomers for each wikiproject in each time interval
//        util.runQuery("SELECT nwikiproject," +
//                        "tcount," +
//                        "COUNT(UNIQUE(user_id)) AS nbr_exp_newcomers," +
//                        "FROM " + util.tableName(defaultDataset, "script_newcomer_prior_wp_experience" + timeIntervalUnit + "edit" + monthlyEditingThreshold) +
//                        "WHERE has_prior_wp = 1 " +
//                        "GROUP BY nwikiproject, tcount",
//                TableId.of(defaultDataset, "script_wp_experienced_newcomers" + timeIntervalUnit + "edit" + monthlyEditingThreshold));
//
//        // fill with the full range of the project
//        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
//                        "t1.tcount AS tcount," +
//                        "IF(t2.nwikiproject IS NULL AND t2.tcount IS NULL, 0, t2.nbr_exp_newcomers) AS nbr_exp_newcomers," +
//                        "FROM " + util.tableName(defaultDataset, "script_user_wp_revs_45_valid_users_wps_valid_range" + timeIntervalUnit, "t1") +
//                        "LEFT JOIN " + util.tableName(defaultDataset, "script_wp_experienced_newcomers" + timeIntervalUnit + "edit" + monthlyEditingThreshold, "t2") +
//                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount",
//                TableId.of(defaultDataset, "script_wp_experienced_newcomers_full_tcounts" + timeIntervalUnit + "edit" + monthlyEditingThreshold));
//
//        // compute for leavers
//        // select the prior wikiprojects joined before joining the focal project
//        util.runQuery("SELECT t1.user_id AS user_id," +
//                        "t1.nwikiproject AS nwikiproject," +
//                        "t1.last_tcount AS focal_last_tcount," +
//                        "t2.nwikiproject AS prior_nwikiproject," +
//                        "FROM " + util.tableName(defaultDataset, "script_user_wp_active_range_revs45" + timeIntervalUnit + "edit" + monthlyEditingThreshold, "t1") +
//                        "INNER JOIN " + util.tableName(defaultDataset, "script_user_wp_active_range_revs45" + timeIntervalUnit + "edit" + monthlyEditingThreshold, "t2") +
//                        "ON t1.user_id = t2.user_id " +
//                        "WHERE t1.last_tcount > t2.tcount",
//                TableId.of(defaultDataset, "script_user_wp_leaving_prior_wps" + timeIntervalUnit + "edit" + monthlyEditingThreshold));
//
//        util.runQuery("SELECT user_id," +
//                        "nwikiproject," +
//                        "COUNT(*) AS prior_wp_cnt," +
//                        "FROM " + util.tableName(defaultDataset, "script_user_wp_leaving_prior_wps" + timeIntervalUnit + "edit" + monthlyEditingThreshold) +
//                        "GROUP BY user_id, nwikiproject " +
//                        "ORDER BY user_id, nwikiproject",
//                TableId.of(defaultDataset, "script_user_leaving_wp_prior_wps_cnt" + timeIntervalUnit + "edit" + monthlyEditingThreshold));
//
//        util.runQuery("SELECT t1.user_id AS user_id," +
//                        "t1.nwikiproject AS nwikiproject," +
////                        "IFNULL(t2.prior_wp_cnt, 0) AS prior_wp_cnt," +
//                        "IF(t2.user_id IS NULL AND t2.nwikiproject IS NULL, 0, t2.prior_wp_cnt) AS prior_wp_cnt," +
//                        "FROM " + util.tableName(defaultDataset, "script_user_wp_first_last_tcounts" + timeIntervalUnit + "edits" + monthlyEditingThreshold, "t1") +
//                        "LEFT JOIN " + util.tableName(defaultDataset, "script_user_leaving_wp_prior_wps_cnt" + timeIntervalUnit + "edit" + monthlyEditingThreshold, "t2") +
//                        "ON t1.user_id = t2.user_id AND t1.nwikiproject = t2.nwikiproject",
//                TableId.of(defaultDataset, "script_user_leaving_wp_prior_wps_cnt_full" + timeIntervalUnit + "edit" + monthlyEditingThreshold));
//
//        // identify leavers
//        util.runQuery("SELECT user_id," +
//                        "nwikiproject," +
//                        "tcount," +
//                        "FROM " + util.tableName(defaultDataset, "script_user_wp_active_range_revs45" + timeIntervalUnit + "edit" + monthlyEditingThreshold) +
//                        "WHERE tcount = last_tcount",
//                TableId.of(defaultDataset, "script_wp_leaver_tcount" + timeIntervalUnit+"edit"+monthlyEditingThreshold));
//
//        // binarize the number of projects joined for the editor has or doesn't have prior experience for newcomers
//        util.runQuery("SELECT t1.user_id AS user_id," +
//                        "t1.nwikiproject AS nwikiproject," +
//                        "t1.tcount AS tcount," +
//                        "t2.prior_wp_cnt AS prior_wp_cnt," +
//                        "IF(t2.prior_wp_cnt > 0, 1, 0) AS has_prior_wp," +
//                        "FROM " + util.tableName(defaultDataset, "script_wp_leaver_tcount" + timeIntervalUnit + "edit" + monthlyEditingThreshold, "t1") +
//                        "INNER JOIN " + util.tableName(defaultDataset, "script_user_leaving_wp_prior_wps_cnt_full" + timeIntervalUnit + "edit" + monthlyEditingThreshold, "t2") +
//                        "ON t1.user_id = t2.user_id AND t1.nwikiproject = t2.nwikiproject",
//                TableId.of(defaultDataset, "script_leaver_prior_wp_experience" + timeIntervalUnit + "edit" + monthlyEditingThreshold));
//
//        // # of experienced leavers for each wikiproject in each time interval
//        util.runQuery("SELECT nwikiproject," +
//                        "tcount," +
//                        "COUNT(UNIQUE(user_id)) AS nbr_exp_leavers," +
//                        "FROM " + util.tableName(defaultDataset, "script_leaver_prior_wp_experience" + timeIntervalUnit + "edit" + monthlyEditingThreshold) +
//                        "WHERE has_prior_wp = 1 " +
//                        "GROUP BY nwikiproject, tcount",
//                TableId.of(defaultDataset, "script_wp_experienced_leavers" + timeIntervalUnit + "edit" + monthlyEditingThreshold));
//
        // fill with the full range of the project
        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "(t1.tcount+1) AS leaving_tcount," +
                        "IF(t2.nwikiproject IS NULL AND t2.tcount IS NULL, 0, t2.nbr_exp_leavers) AS nbr_committed_leavers," +
                        "FROM " + util.tableName(defaultDataset, "script_user_wp_revs_45_valid_users_wps_valid_range" + timeIntervalUnit, "t1") +
                        "LEFT JOIN " + util.tableName(defaultDataset, "script_wp_experienced_leavers" + timeIntervalUnit + "edit" + monthlyEditingThreshold, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount",
                TableId.of(defaultDataset, "script_wp_committed_leavers_full_tcounts" + timeIntervalUnit + "edit" + monthlyEditingThreshold));

        // merge into the updated result table
        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.wikiproject AS wikiproject," +
                        "t1.tcount AS tcount," +
                        "t1.pre_tcount AS pre_tcount," +
                        "t1.newbies_nbr AS newbies_nbr," +
                        "t1.newcomers_nbr AS newcomers_nbr," +
                        "IFNULL(t2.nbr_exp_newcomers, 0) AS nbr_committed_newcomers," +
                        "t1.leavers_nbr AS leavers_nbr," +
                        "t1.high_short_leavers_nbr AS high_short_leavers_nbr," +
                        "t1.high_long_leavers_nbr AS high_long_leavers_nbr," +
                        "t1.low_short_leavers_nbr AS low_short_leavers_nbr," +
                        "t1.low_long_leavers_nbr AS low_long_leavers_nbr," +
                        "t1.remainings_nbr AS remainings_nbr," +
                        "t1.remainings_prod0 AS remainings_prod0," +
                        "t1.remainings_coors45 AS remainings_coors45," +
                        "t1.remainings_art_comm1 AS remainings_art_comm1," +
                        "t1.remainings_user_comm3 AS remainings_user_comm3," +
                        "t1.superficial_leavers_cnt AS superficial_leavers_cnt," +
                        "t1.substantive_leavers_cnt AS substantive_leavers_cnt," +
                        "t1.wp_tenure AS wp_tenure," +
                        "t1.avg_substantive_leaver_tenure AS avg_substantive_leaver_tenure," +
                        "t1.avg_last_tcount_ns0_edits AS avg_last_tcount_ns0_edits," +
                        "t1.avg_last_tcount_ns45_edits AS avg_last_tcount_ns45_edits," +
                        "t1.project_prod0 AS project_prod0," +
                        "t1.project_coor4 AS project_coor4," +
                        "t1.project_coor5 AS project_coor5," +
                        "t1.project_coors45 AS project_coors45," +
                        "t1.project_art_comm1 AS project_art_comm1," +
                        "t1.project_user_comm3 AS project_user_comm3," +
                        "t1.pre_project_prod0 AS pre_project_prod0," +
                        "t1.pre_project_coors45 AS pre_project_coors45," +
                        "t1.delta_prod0 AS delta_prod0," +
                        "t1.delta_coors45 AS delta_coors45," +
                        "FROM " + util.tableName(defaultDataset, "script_results_wp_ivs_cvs_group_abv0_dltDVs_all_newcomers_leavers"+timeIntervalUnit+"edit"+monthlyEditingThreshold, "t1") +
                        "INNER JOIN " + util.tableName(defaultDataset, "script_wp_experienced_newcomers_full_tcounts"+timeIntervalUnit+"edit"+monthlyEditingThreshold, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount " +
                        "ORDER BY nwikiproject, tcount",
                TableId.of(defaultDataset, "script_results_wp_ivs_cvs_group_abv0_dltDVs_committed_newcomers"+timeIntervalUnit+"edit"+monthlyEditingThreshold));


        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.wikiproject AS wikiproject," +
                        "t1.tcount AS tcount," +
                        "t1.pre_tcount AS pre_tcount," +
                        "t1.newbies_nbr AS newbies_nbr," +
                        "t1.newcomers_nbr AS newcomers_nbr," +
                        "t1.nbr_committed_newcomers AS nbr_committed_newcomers," +
                        "IFNULL(t2.nbr_committed_leavers, 0) AS nbr_committed_leavers," +
                        "t1.leavers_nbr AS leavers_nbr," +
                        "t1.high_short_leavers_nbr AS high_short_leavers_nbr," +
                        "t1.high_long_leavers_nbr AS high_long_leavers_nbr," +
                        "t1.low_short_leavers_nbr AS low_short_leavers_nbr," +
                        "t1.low_long_leavers_nbr AS low_long_leavers_nbr," +
                        "t1.remainings_nbr AS remainings_nbr," +
                        "t1.remainings_prod0 AS remainings_prod0," +
                        "t1.remainings_coors45 AS remainings_coors45," +
                        "t1.remainings_art_comm1 AS remainings_art_comm1," +
                        "t1.remainings_user_comm3 AS remainings_user_comm3," +
                        "t1.superficial_leavers_cnt AS superficial_leavers_cnt," +
                        "t1.substantive_leavers_cnt AS substantive_leavers_cnt," +
                        "t1.wp_tenure AS wp_tenure," +
                        "t1.avg_substantive_leaver_tenure AS avg_substantive_leaver_tenure," +
                        "t1.avg_last_tcount_ns0_edits AS avg_last_tcount_ns0_edits," +
                        "t1.avg_last_tcount_ns45_edits AS avg_last_tcount_ns45_edits," +
                        "t1.project_prod0 AS project_prod0," +
                        "t1.project_coor4 AS project_coor4," +
                        "t1.project_coor5 AS project_coor5," +
                        "t1.project_coors45 AS project_coors45," +
                        "t1.project_art_comm1 AS project_art_comm1," +
                        "t1.project_user_comm3 AS project_user_comm3," +
                        "t1.pre_project_prod0 AS pre_project_prod0," +
                        "t1.pre_project_coors45 AS pre_project_coors45," +
                        "t1.delta_prod0 AS delta_prod0," +
                        "t1.delta_coors45 AS delta_coors45," +
                        "FROM " + util.tableName(defaultDataset, "script_results_wp_ivs_cvs_group_abv0_dltDVs_committed_newcomers"+timeIntervalUnit+"edit"+monthlyEditingThreshold, "t1") +
                        "INNER JOIN " + util.tableName(defaultDataset, "script_wp_committed_leavers_full_tcounts"+timeIntervalUnit+"edit"+monthlyEditingThreshold, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.leaving_tcount " +
                        "ORDER BY nwikiproject, tcount",
                TableId.of(defaultDataset, "script_results_wp_ivs_cvs_group_abv0_dltDVs_committed_newcomers_leavers"+timeIntervalUnit+"edit"+monthlyEditingThreshold));


    }

    private void addSuperficialLastTcountEditsToResults() throws Exception {

        // # of edits before the left (edits in the previous tcount), specifying on superficial leavers
        util.runQuery("SELECT t1.user_id AS user_id," +
                        "t1.nwikiproject AS nwikiproject," +
                        "(t1.tcount+1) AS tcount," +
                        "t1.article_edits AS article_edits," +
                        "FROM " + util.tableName(defaultDataset, "script_editor_wp_tcount_ns0_edits"+timeIntervalUnit, "t1") +
                        "INNER JOIN " + util.tableName(defaultDataset, "script_user_wp_first_last_tcounts"+timeIntervalUnit+"edits"+monthlyEditingThreshold, "t2") +
                        "ON t1.user_id = t2.user_id AND t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.last_tcount " +
                        "WHERE (t2.first_tcount+1) = t2.last_tcount",
                TableId.of(defaultDataset, "script_superficial_leavers_last_tcount_ns0_edits"+timeIntervalUnit+"edit"+monthlyEditingThreshold));
        // long tail distribution

        // Group by wikiproject and tcount to gain the average
        util.runQuery("SELECT nwikiproject," +
                        "tcount," +
                        "AVG(article_edits) AS avg_last_tcount_ns0_edits," +
                        "COUNT(*) AS nbr_superficial_leavers," +
                        "FROM " + util.tableName(defaultDataset, "script_superficial_leavers_last_tcount_ns0_edits"+timeIntervalUnit+"edit"+monthlyEditingThreshold) +
                        "GROUP BY nwikiproject, tcount",
                TableId.of(defaultDataset, "script_avg_superficial_leavers_last_tcount_ns0_edits"+timeIntervalUnit+"edit"+monthlyEditingThreshold));

        // editors' average edits on ns45 per project per tcount
        util.runQuery("SELECT t1.user_id AS user_id," +
                        "t1.nwikiproject AS nwikiproject," +
                        "t2.tcount AS tcount," +
                        "COUNT(*) AS coors_edits," +
                        "FROM " + util.tableName("bowen_wikis_quitting", "rev_ns45_user_wikiproject_ts_encoded_full", "t1") +
                        "INNER JOIN " + util.tableName(defaultDataset, "script_user_wp_revs_45_valid_users_wps_valid_range"+timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject " +
                        "WHERE t1.timestamp >= t2.start_ts AND t1.timestamp < t2.end_ts " +
                        "GROUP BY user_id, nwikiproject, tcount",
                TableId.of(defaultDataset, "script_editor_wp_coor45_edits"+timeIntervalUnit));

        // # of edits before the left (edits in the previous tcount), specifying on superficial leavers
        util.runQuery("SELECT t1.user_id AS user_id," +
                        "t1.nwikiproject AS nwikiproject," +
                        "(t1.tcount+1) AS tcount," +
                        "t1.coors_edits AS coors_edits," +
                        "FROM " + util.tableName(defaultDataset, "script_editor_wp_coor45_edits"+timeIntervalUnit, "t1") +
                        "INNER JOIN " + util.tableName(defaultDataset, "script_user_wp_first_last_tcounts"+timeIntervalUnit+"edits"+monthlyEditingThreshold, "t2") +
                        "ON t1.user_id = t2.user_id AND t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.last_tcount " +
                        "WHERE (t2.first_tcount+1) = t2.last_tcount",
                TableId.of(defaultDataset, "script_superficial_leavers_last_tcount_ns45_edits"+timeIntervalUnit+"edit"+monthlyEditingThreshold));
        // long tail distribution

        // Group by wikiproject and tcount to gain the average
        util.runQuery("SELECT nwikiproject," +
                        "tcount," +
                        "AVG(coors_edits) AS avg_last_tcount_ns45_edits," +
                        "COUNT(*) AS nbr_superficial_leavers," +
                        "FROM " + util.tableName(defaultDataset, "script_superficial_leavers_last_tcount_ns45_edits"+timeIntervalUnit+"edit"+monthlyEditingThreshold) +
                        "GROUP BY nwikiproject, tcount",
                TableId.of(defaultDataset, "script_avg_superficial_leavers_last_tcount_ns45_edits"+timeIntervalUnit+"edit"+monthlyEditingThreshold));

        // merge with ns 0 and identify the last tcounts
        mergeSuperficialLastTcountEditsToResults();
    }

    private void mergeSuperficialLastTcountEditsToResults() throws Exception {

        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.wikiproject AS wikiproject," +
                        "t1.tcount AS tcount," +
                        "t1.newcomers_nbr AS newcomers_nbr," +
                        "t1.leavers_nbr AS leavers_nbr," +
                        "t1.remainings_nbr AS remainings_nbr," +
                        "t1.remainings_prod0 AS remainings_prod0," +
                        "t1.remainings_coors45 AS remainings_coors45," +
                        "t1.remainings_art_comm1 AS remainings_art_comm1," +
                        "t1.remainings_user_comm3 AS remainings_user_comm3," +
                        "t1.superficial_leavers_cnt AS superficial_leavers_cnt," +
                        "t1.substantive_leavers_cnt AS substantive_leavers_cnt," +
                        "t1.wp_tenure AS wp_tenure," +
                        "t1.avg_substantive_leaver_tenure AS avg_substantive_leaver_tenure," +
                        "IFNULL(t2.avg_last_tcount_ns0_edits, 0) AS avg_last_tcount_ns0_edits," +
                        "t1.project_prod0 AS project_prod0," +
                        "t1.project_coor4 AS project_coor4," +
                        "t1.project_coor5 AS project_coor5," +
                        "t1.project_coors45 AS project_coors45," +
                        "t1.project_art_comm1 AS project_art_comm1," +
                        "t1.project_user_comm3 AS project_user_comm3," +
                        "FROM " + util.tableName(defaultDataset, "script_results_wp_ivs_cvs_group_ns01345_45_abv0"+timeIntervalUnit+"edit"+monthlyEditingThreshold, "t1") +
                        "LEFT JOIN " + util.tableName(defaultDataset, "script_avg_superficial_leavers_last_tcount_ns0_edits"+timeIntervalUnit+"edit"+monthlyEditingThreshold, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount " +
                        "ORDER BY nwikiproject, tcount",
                TableId.of(defaultDataset, "script_results_wp_ivs_cvs_group_abv0_ltcount0"+timeIntervalUnit+"edit"+monthlyEditingThreshold));


        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.wikiproject AS wikiproject," +
                        "t1.tcount AS tcount," +
                        "t1.newcomers_nbr AS newcomers_nbr," +
                        "t1.leavers_nbr AS leavers_nbr," +
                        "t1.remainings_nbr AS remainings_nbr," +
                        "t1.remainings_prod0 AS remainings_prod0," +
                        "t1.remainings_coors45 AS remainings_coors45," +
                        "t1.remainings_art_comm1 AS remainings_art_comm1," +
                        "t1.remainings_user_comm3 AS remainings_user_comm3," +
                        "t1.superficial_leavers_cnt AS superficial_leavers_cnt," +
                        "t1.substantive_leavers_cnt AS substantive_leavers_cnt," +
                        "t1.wp_tenure AS wp_tenure," +
                        "t1.avg_substantive_leaver_tenure AS avg_substantive_leaver_tenure," +
                        "t1.avg_last_tcount_ns0_edits AS avg_last_tcount_ns0_edits," +
                        "IFNULL(t2.avg_last_tcount_ns45_edits, 0) AS avg_last_tcount_ns45_edits," +
                        "t1.project_prod0 AS project_prod0," +
                        "t1.project_coor4 AS project_coor4," +
                        "t1.project_coor5 AS project_coor5," +
                        "t1.project_coors45 AS project_coors45," +
                        "t1.project_art_comm1 AS project_art_comm1," +
                        "t1.project_user_comm3 AS project_user_comm3," +
                        "FROM " + util.tableName(defaultDataset, "script_results_wp_ivs_cvs_group_abv0_ltcount0"+timeIntervalUnit+"edit"+monthlyEditingThreshold, "t1") +
                        "LEFT JOIN " + util.tableName(defaultDataset, "script_avg_superficial_leavers_last_tcount_ns45_edits"+timeIntervalUnit+"edit"+monthlyEditingThreshold, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount " +
                        "ORDER BY nwikiproject, tcount",
                TableId.of(defaultDataset, "script_results_wp_ivs_cvs_group_abv0_ltcount045"+timeIntervalUnit+"edit"+monthlyEditingThreshold));
    }

    private void addProjectDeltaDVsToResults() throws Exception {

        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.wikiproject AS wikiproject," +
                        "t1.tcount AS tcount," +
                        "(t1.tcount-1) AS pre_tcount," +
                        "t1.newcomers_nbr AS newcomers_nbr," +
                        "t1.leavers_nbr AS leavers_nbr," +
                        "t1.remainings_nbr AS remainings_nbr," +
                        "t1.remainings_prod0 AS remainings_prod0," +
                        "t1.remainings_coors45 AS remainings_coors45," +
                        "t1.remainings_art_comm1 AS remainings_art_comm1," +
                        "t1.remainings_user_comm3 AS remainings_user_comm3," +
                        "t1.superficial_leavers_cnt AS superficial_leavers_cnt," +
                        "t1.substantive_leavers_cnt AS substantive_leavers_cnt," +
                        "t1.wp_tenure AS wp_tenure," +
                        "t1.avg_substantive_leaver_tenure AS avg_substantive_leaver_tenure," +
                        "t1.avg_last_tcount_ns0_edits AS avg_last_tcount_ns0_edits," +
                        "IFNULL(t2.avg_last_tcount_ns45_edits, 0) AS avg_last_tcount_ns45_edits," +
                        "t1.project_prod0 AS project_prod0," +
                        "t1.project_coor4 AS project_coor4," +
                        "t1.project_coor5 AS project_coor5," +
                        "t1.project_coors45 AS project_coors45," +
                        "t1.project_art_comm1 AS project_art_comm1," +
                        "t1.project_user_comm3 AS project_user_comm3," +
                        "FROM " + util.tableName(defaultDataset, "script_results_wp_ivs_cvs_group_abv0_ltcount0"+timeIntervalUnit+"edit"+monthlyEditingThreshold, "t1") +
                        "LEFT JOIN " + util.tableName(defaultDataset, "script_avg_superficial_leavers_last_tcount_ns45_edits"+timeIntervalUnit+"edit"+monthlyEditingThreshold, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount " +
                        "ORDER BY nwikiproject, tcount",
                TableId.of(defaultDataset, "script_results_wp_ivs_cvs_group_abv0_ltcount045"+timeIntervalUnit+"edit"+monthlyEditingThreshold));

        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.wikiproject AS wikiproject," +
                        "t1.tcount AS tcount," +
                        "(t1.tcount-1) AS pre_tcount," +
                        "t1.newcomers_nbr AS newcomers_nbr," +
                        "t1.leavers_nbr AS leavers_nbr," +
                        "t1.remainings_nbr AS remainings_nbr," +
                        "t1.remainings_prod0 AS remainings_prod0," +
                        "t1.remainings_coors45 AS remainings_coors45," +
                        "t1.remainings_art_comm1 AS remainings_art_comm1," +
                        "t1.remainings_user_comm3 AS remainings_user_comm3," +
                        "t1.superficial_leavers_cnt AS superficial_leavers_cnt," +
                        "t1.substantive_leavers_cnt AS substantive_leavers_cnt," +
                        "t1.wp_tenure AS wp_tenure," +
                        "t1.avg_substantive_leaver_tenure AS avg_substantive_leaver_tenure," +
                        "t1.avg_last_tcount_ns0_edits AS avg_last_tcount_ns0_edits," +
                        "t2.avg_last_tcount_ns45_edits AS avg_last_tcount_ns45_edits," +
                        "t1.project_prod0 AS project_prod0," +
                        "t1.project_coor4 AS project_coor4," +
                        "t1.project_coor5 AS project_coor5," +
                        "t1.project_coors45 AS project_coors45," +
                        "t1.project_art_comm1 AS project_art_comm1," +
                        "t1.project_user_comm3 AS project_user_comm3," +
                        "t2.project_prod0 AS pre_project_prod0," +
                        "t2.project_coors45 AS pre_project_coors45," +
                        "(t1.project_prod0 - t2.project_prod0) AS delta_prod0," +
                        "(t1.project_coors45 - t2.project_coors45) AS delta_coors45," +
                        "FROM " + util.tableName(defaultDataset, "script_results_wp_ivs_cvs_group_abv0_ltcount045"+timeIntervalUnit+"edit"+monthlyEditingThreshold, "t1") +
                        "INNER JOIN " + util.tableName(defaultDataset, "script_results_wp_ivs_cvs_group_abv0_ltcount045"+timeIntervalUnit+"edit"+monthlyEditingThreshold, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.pre_tcount = t2.tcount " +
                        "ORDER BY nwikiproject, tcount",
                TableId.of(defaultDataset, "script_results_wp_ivs_cvs_group_abv0_dltDVs"+timeIntervalUnit+"edit"+monthlyEditingThreshold));

    }

    private void identifySepcialSubstantivesOnProjectPageWPTcount() throws Exception {

        // Table schema: nwikiproject, tcount, user_text, edit_page, timestamp
//        PrintWriter writer = new PrintWriter(superficialLeaverAnalysisFile, "UTF-8");
        int recordCnt = 0;

        BufferedWriter bw = new BufferedWriter(new FileWriter(superficialLeaverAnalysisFile));
        bw.write("wikiproject,tcount,user,ns,timestamp\n");

//        writer.printf("wikiproject,tcount,user,article,timestamp\n");

        QueryResponse responseWPTcount = util.runQuery("SELECT nwikiproject," +
                        "tcount," +
                        "superficial_leavers_cnt," +
                        "FROM " + util.tableName(defaultDataset, "script_results_wp_ivs_cvs_group_ns01345_45_abv03edit5") +
                        "WHERE substantive_leavers_cnt > 0.15*(remainings_nbr+1) AND substantive_leavers_cnt < 0.2*(remainings_nbr+1) " +
                        "ORDER BY substantive_leavers_cnt",
//                        "ORDER BY nwikiproject, tcount, superficial_leavers_cnt",
                TableId.of(defaultDataset, "substantive_leaver_analysis_identifying_wp_tcount"));

        QueryResult result = responseWPTcount.result();
        Iterator<List<FieldValue>> iter = result.iterateAll();

        // Identify wikiproject and tcount
        while (iter.hasNext()) {
            List<FieldValue> row = iter.next();
            // nwikiproject, tcount, superficial_leavers_cnt
            String nwikiproject = row.get(0).stringValue();
            int tcount = Integer.parseInt(row.get(1).stringValue());

            // Identify the superficial leavers
            QueryResponse responseLeavers = util.runQuery("SELECT user_id," +
                    "nwikiproject," +
                    "tcount," +
                    "FROM " + util.tableName(defaultDataset, "script_user_wp_active_range_revs453edit5") +
                    "WHERE nwikiproject="+nwikiproject + " AND tcount="+(tcount-1) + " AND tcount != first_tcount AND (tcount+1) = leaving_tcount");

            QueryResult resultLeavers = responseLeavers.result();
            Iterator<List<FieldValue>> iterLeavers = resultLeavers.iterateAll();

            while (iterLeavers.hasNext()) {
                List<FieldValue> rowLeavers = iterLeavers.next();
                String user_id = rowLeavers.get(0).stringValue();

                // Identify the timestamp of leavers' edits
                QueryResponse responseEdits = util.runQuery("SELECT user_text," +
                        "wikiproject," +
                        "timestamp," +
                        "ns," +
                        "FROM " + util.tableName(defaultDataset, "script_user_wp_revs_45_valid_users_wps") +
                        "WHERE user_id=" + user_id + " AND nwikiproject=" + nwikiproject);

                QueryResult resultEdits = responseEdits.result();
                Iterator<List<FieldValue>> iterEdits = resultEdits.iterateAll();

                // Extract all the info - user_text, wikiproject, timestamp, ns
                while (iterEdits.hasNext()) {
                    List<FieldValue> lowEdits = iterEdits.next();
                    String user_text = lowEdits.get(0).stringValue();
                    String wikiproject = lowEdits.get(1).stringValue();
                    long timestamp = lowEdits.get(2).longValue();
                    String editDate = getEditDate(timestamp*1000);
                    String ns = lowEdits.get(3).stringValue();

                    bw.write("WikiProject:"+wikiproject+","+tcount+","+user_text+","+ns+","+editDate+"\n");
//                        writer.printf("%s,%d,%s,%s,%s\n", wikiproject, tcount, user_text, article, editDate);
                    System.out.println("Record #: " + (++recordCnt));

                    if (recordCnt >= recordThreshold) {
                        bw.close();
                        return;
                    }
                }

            }
        }
//        writer.close();
        bw.close();
    }

    private void identifySepcialSuperficialsOnProjectPageWPTcount() throws Exception {

        // Table schema: nwikiproject, tcount, user_text, edit_page, timestamp
//        PrintWriter writer = new PrintWriter(superficialLeaverAnalysisFile, "UTF-8");
        int recordCnt = 0;

        BufferedWriter bw = new BufferedWriter(new FileWriter(superficialLeaverAnalysisFile));
        bw.write("wikiproject,tcount,user,ns,timestamp\n");

//        writer.printf("wikiproject,tcount,user,article,timestamp\n");

        QueryResponse responseWPTcount = util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t2.wikiproject AS wikiproject," +
                        "t1.tcount AS tcount," +
                        "t1.superficial_leavers_cnt AS superficial_leavers_cnt," +
                        "FROM " + util.tableName(defaultDataset, "script_results_wp_ivs_cvs_group_ns013453edit5_above0pct", "t1") +
                        "INNER JOIN " + util.tableName(defaultDataset, "wp_full_valid_tcounts_above50", "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject " +
                        "WHERE t1.superficial_leavers_cnt <= 0.2*(t1.remainings_nbr+1) AND t1.superficial_leavers_cnt >= 0.1*(t1.remainings_nbr+1) AND t1.remainings_nbr > 0 " +
                        "ORDER BY t1.superficial_leavers_cnt",
//                        "ORDER BY nwikiproject, tcount, superficial_leavers_cnt",
                TableId.of(defaultDataset, "superficial_leaver_analysis_identifying_wp_tcount"));

        QueryResult result = responseWPTcount.result();
        Iterator<List<FieldValue>> iter = result.iterateAll();

        // Identify wikiproject and tcount
        while (iter.hasNext()) {
            List<FieldValue> row = iter.next();
            // nwikiproject, tcount, superficial_leavers_cnt
            String nwikiproject = row.get(0).stringValue();
            int tcount = Integer.parseInt(row.get(2).stringValue());

            // Identify the superficial leavers
            QueryResponse responseLeavers = util.runQuery("SELECT user_id," +
                    "nwikiproject," +
                    "tcount," +
                    "FROM " + util.tableName(defaultDataset, "script_user_wp_active_range_revs453edit5") +
                    "WHERE nwikiproject="+nwikiproject + " AND tcount="+(tcount-1) + " AND tcount = first_tcount AND (tcount+1) = leaving_tcount");

            QueryResult resultLeavers = responseLeavers.result();
            Iterator<List<FieldValue>> iterLeavers = resultLeavers.iterateAll();

            while (iterLeavers.hasNext()) {
                List<FieldValue> rowLeavers = iterLeavers.next();
                String user_id = rowLeavers.get(0).stringValue();

                // Identify the timestamp of leavers' edits
                QueryResponse responseEdits = util.runQuery("SELECT user_text," +
                        "wikiproject," +
                        "timestamp," +
                        "ns," +
                        "FROM " + util.tableName(defaultDataset, "script_user_wp_revs_45_valid_users_wps") +
                        "WHERE user_id=" + user_id + " AND nwikiproject=" + nwikiproject);

                QueryResult resultEdits = responseEdits.result();
                Iterator<List<FieldValue>> iterEdits = resultEdits.iterateAll();

                // Extract all the info - user_text, wikiproject, timestamp, ns
                while (iterEdits.hasNext()) {
                    List<FieldValue> lowEdits = iterEdits.next();
                    String user_text = lowEdits.get(0).stringValue();
                    String wikiproject = lowEdits.get(1).stringValue();
                    long timestamp = lowEdits.get(2).longValue();
                    String editDate = getEditDate(timestamp*1000);
                    String ns = lowEdits.get(3).stringValue();

                    bw.write("WikiProject:"+wikiproject+","+tcount+","+user_text+","+ns+","+editDate+"\n");
//                        writer.printf("%s,%d,%s,%s,%s\n", wikiproject, tcount, user_text, article, editDate);
                    System.out.println("Record #: " + (++recordCnt));

                    if (recordCnt >= recordThreshold) {
                        bw.close();
                        return;
                    }
                }

            }
        }
//        writer.close();
        bw.close();
    }

    private void identifySpecialSuperficialLeaversOnArtcilesWPTcount() throws Exception {

        // Table schema: nwikiproject, tcount, user_text, edit_page, timestamp
//        PrintWriter writer = new PrintWriter(superficialLeaverAnalysisFile, "UTF-8");
        int recordCnt = 0;

        BufferedWriter bw = new BufferedWriter(new FileWriter(superficialLeaverAnalysisFile));
        bw.write("wikiproject,tcount,user,article,timestamp\n");

//        writer.printf("wikiproject,tcount,user,article,timestamp\n");

//        QueryResponse responseWPTcount = util.runQuery("SELECT nwikiproject," +
//                        "tcount," +
//                        "superficial_leavers_cnt," +
//                        "FROM " + util.tableName(defaultDataset, "script_results_wp_ivs_cvs_group_ns013453edit5") +
//                        "WHERE superficial_leavers_cnt > 0.4*remainings_nbr AND remainings_nbr > 0 " +
//                        "ORDER BY nwikiproject, tcount, superficial_leavers_cnt",
//                TableId.of(defaultDataset, "superficial_leaver_analysis_identifying_wp_tcount"));

        QueryResponse responseWPTcount = util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t2.wikiproject AS wikiproject," +
                        "t1.tcount AS tcount," +
                        "t1.superficial_leavers_cnt AS superficial_leavers_cnt," +
                        "FROM " + util.tableName(defaultDataset, "script_results_wp_ivs_cvs_group_ns013453edit5", "t1") +
                        "INNER JOIN " + util.tableName(defaultDataset, "wp_full_valid_tcounts_above50", "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject " +
                        "WHERE t1.superficial_leavers_cnt > 0.4*t1.remainings_nbr AND t1.remainings_nbr > 0 " +
                        "ORDER BY t1.superficial_leavers_cnt",
//                        "ORDER BY nwikiproject, tcount, superficial_leavers_cnt",
                TableId.of(defaultDataset, "superficial_leaver_analysis_identifying_wp_tcount_ns0"));

        QueryResult result = responseWPTcount.result();
        Iterator<List<FieldValue>> iter = result.iterateAll();

        // Identify wikiproject and tcount
        while (iter.hasNext()) {
            List<FieldValue> row = iter.next();
            // nwikiproject, tcount, superficial_leavers_cnt
            String nwikiproject = row.get(0).stringValue();
            String wikiproject = row.get(1).stringValue();
            int tcount = Integer.parseInt(row.get(2).stringValue());

            // Query the project name by project id
//            QueryResponse responseWP = util.runQuery("SELECT wikiproject," +
//                            "FROM " + util.tableName("bowen_editor_attachments", "wikiprojects_valid_withIds_3members") +
//                            "WHERE nwikiproject=" + nwikiproject);
//
//            QueryResult resultWP = responseWP.result();
//            Iterator<List<FieldValue>> iterWP = resultWP.iterateAll();
//
//            String wikiproject = null;
//            while (iterWP.hasNext()) {
//                List<FieldValue> rowWP = iterWP.next();
//                wikiproject = rowWP.get(0).stringValue();
//            }

            // Identify the superficial leavers
            QueryResponse responseLeavers = util.runQuery("SELECT user_id," +
                            "nwikiproject," +
                            "tcount," +
                            "FROM " + util.tableName(defaultDataset, "script_user_wp_active_range_revs453edit5") +
                            "WHERE nwikiproject="+nwikiproject + " AND tcount="+(tcount-1) + " AND tcount = first_tcount AND (tcount+1) = leaving_tcount");

            QueryResult resultLeavers = responseLeavers.result();
            Iterator<List<FieldValue>> iterLeavers = resultLeavers.iterateAll();

            while (iterLeavers.hasNext()) {
                List<FieldValue> rowLeavers = iterLeavers.next();
                String user_id = rowLeavers.get(0).stringValue();

                // Identify the timestamp of leavers' edits
                QueryResponse responseEdits = util.runQuery("SELECT user_text," +
                                "user_id," +
                                "wikiproject," +
                                "nwikiproject," +
                                "timestamp," +
                                "FROM " + util.tableName("bowen_user_dropouts", "rev_ns0_valid_user_wikiproject_std") +
                                "WHERE user_id=" + user_id + " AND nwikiproject=" + nwikiproject);

                QueryResult resultEdits = responseEdits.result();
                Iterator<List<FieldValue>> iterEdits = resultEdits.iterateAll();

                // identify the edits of the article
                while (iterEdits.hasNext()) {
                    List<FieldValue> lowEdits = iterEdits.next();
                    String user_text = lowEdits.get(0).stringValue();
                    long timestamp = lowEdits.get(4).longValue();

                    QueryResponse responseArticles = util.runQuery("SELECT page_title," +
                                    "user_text," +
                                    "ns, " +
                                    "timestamp," +
                                    "FROM " + util.tableName("bowen_user_dropouts", "rev_lower_ns0") +
                                    "WHERE user_text=\'"+user_text + "\' AND timestamp="+timestamp);


                    QueryResult resultArtciles = responseArticles.result();
                    Iterator<List<FieldValue>> iterArticles = resultArtciles.iterateAll();

                    while (iterArticles.hasNext()) {
                        List<FieldValue> lowArticles = iterArticles.next();
                        String article = lowArticles.get(0).stringValue();
                        String editDate = getEditDate(timestamp*1000);

                        // output the results
                        bw.write(wikiproject+","+tcount+","+user_text+","+article+","+editDate+"\n");
//                        writer.printf("%s,%d,%s,%s,%s\n", wikiproject, tcount, user_text, article, editDate);
                        System.out.println("Record #: " + (++recordCnt));

                        if (recordCnt >= recordThreshold) {
                            bw.close();
                            return;
                        }
                    }
                }

            }
        }
//        writer.close();
        bw.close();
    }

    private static String getEditDate(long timestamp) {
        Date date = new Date(timestamp);
        DateFormat format = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
        format.setTimeZone(TimeZone.getTimeZone("America/Seattle"));
        return format.format(date);
    }

    private void leaverLagEffectsanalysis() throws Exception {

        // Add the next two tcounts
        util.runQuery("SELECT *," +
                        "(tcount+1) AS next_tcount," +
                        "(tcount+2) AS next_next_tcount," +
                        "FROM " + util.tableName(defaultDataset, "script_results_wp_ivs_cvs_group_ns01345"+timeIntervalUnit+"edit"+monthlyEditingThreshold),
                TableId.of(defaultDataset, "script_results_wp_ivs_cvs_lag"+timeIntervalUnit+"edit"+monthlyEditingThreshold));

        // join for tcount+1
        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "t1.next_tcount AS next_tcount," +
                        "t2.wp_tenure AS wp_tenure," +
                        "t1.superficial_leavers_cnt AS superficial_leavers_cnt," +
                        "t1.substantive_leavers_cnt AS substantive_leavers_cnt," +
                        "t1.remainings_nbr AS remainings_nbr," +
                        "t1.avg_substantive_leaver_tenure AS avg_substantive_leaver_tenure," +
                        "t2.newcomers_nbr AS lagging_newcomers_nbr," +
                        "t2.leavers_nbr AS lagging_leavers_nbr," +
                        "t2.remainings_nbr AS lagging_remainings_nbr," +
                        "t2.remainings_prod0 AS lagging_remainings_prod0," +
                        "t2.remainings_coors45 AS lagging_remainings_coors45," +
                        "t2.remainings_art_comm1 AS lagging_remainings_art_comm1," +
                        "t2.remainings_user_comm3 AS lagging_remainings_user_comm3," +
                        "t2.project_prod0 AS lagging_project_prod0," +
                        "t2.project_coors45 AS lagging_project_coors45," +
                        "t2.project_art_comm1 AS lagging_project_art_comm1," +
                        "t2.project_user_comm3 AS lagging_project_user_comm3," +
                        "FROM " + util.tableName(defaultDataset, "script_results_wp_ivs_cvs_lag"+timeIntervalUnit+"edit"+monthlyEditingThreshold, "t1") +
                        "INNER JOIN " + util.tableName(defaultDataset, "script_results_wp_ivs_cvs_lag"+timeIntervalUnit+"edit"+monthlyEditingThreshold, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.next_tcount = t2.tcount " +
                        "ORDER BY nwikiproject, tcount",
                TableId.of(defaultDataset, "script_results_wp_ivs_cvs_lagging_nextt1"+timeIntervalUnit+"edit"+monthlyEditingThreshold));


        // join for tcount+2
        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "t1.next_next_tcount AS next_next_tcount," +
                        "t2.wp_tenure AS wp_tenure," +
                        "t1.superficial_leavers_cnt AS superficial_leavers_cnt," +
                        "t1.substantive_leavers_cnt AS substantive_leavers_cnt," +
                        "t1.remainings_nbr AS remainings_nbr," +
                        "t1.avg_substantive_leaver_tenure AS avg_substantive_leaver_tenure," +
                        "t2.newcomers_nbr AS lagging_newcomers_nbr," +
                        "t2.leavers_nbr AS lagging_leavers_nbr," +
                        "t2.remainings_nbr AS lagging_remainings_nbr," +
                        "t2.remainings_prod0 AS lagging_remainings_prod0," +
                        "t2.remainings_coors45 AS lagging_remainings_coors45," +
                        "t2.remainings_art_comm1 AS lagging_remainings_art_comm1," +
                        "t2.remainings_user_comm3 AS lagging_remainings_user_comm3," +
                        "t2.project_prod0 AS lagging_project_prod0," +
                        "t2.project_coors45 AS lagging_project_coors45," +
                        "t2.project_art_comm1 AS lagging_project_art_comm1," +
                        "t2.project_user_comm3 AS lagging_project_user_comm3," +
                        "FROM " + util.tableName(defaultDataset, "script_results_wp_ivs_cvs_lag"+timeIntervalUnit+"edit"+monthlyEditingThreshold, "t1") +
                        "INNER JOIN " + util.tableName(defaultDataset, "script_results_wp_ivs_cvs_lag"+timeIntervalUnit+"edit"+monthlyEditingThreshold, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.next_next_tcount = t2.tcount " +
                        "ORDER BY nwikiproject, tcount",
                TableId.of(defaultDataset, "script_results_wp_ivs_cvs_lagging_nextt2"+timeIntervalUnit+"edit"+monthlyEditingThreshold));

    }

    private void mergeGroupDVsIntoFullTable() throws Exception {


        // merge with group level productivity
        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "t1.newcomers_nbr AS newcomers_nbr," +
                        "t1.leavers_nbr AS leavers_nbr," +
                        "t1.remainings_nbr AS remainings_nbr," +
                        "t1.remainings_prod0 AS remainings_prod0," +
                        "t1.remainings_coors45 AS remainings_coors45," +
                        "t1.remainings_art_comm1 AS remainings_art_comm1," +
                        "t1.remainings_user_comm3 AS remainings_user_comm3," +
                        "t1.superficial_leavers_cnt AS superficial_leavers_cnt," +
                        "t1.substantive_leavers_cnt AS substantive_leavers_cnt," +
                        "t1.wp_tenure AS wp_tenure," +
                        "t1.avg_substantive_leaver_tenure AS avg_substantive_leaver_tenure," +
                        "IFNULL(t2.group_article_productivity, 0) AS project_prod0," +
                        "FROM " + util.tableName(defaultDataset, "script_results_wp_ivs_cvs"+timeIntervalUnit+"edit"+monthlyEditingThreshold, "t1") +
                        "LEFT JOIN " + util.tableName(defaultDataset, "dv_wp_full_mbr_prod0_per_time_interval_months" + timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount",
                TableId.of(defaultDataset, "script_results_wp_ivs_cvs_group_ns0"+timeIntervalUnit+"edit"+monthlyEditingThreshold));


        // merge with group level coordination
        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "t1.newcomers_nbr AS newcomers_nbr," +
                        "t1.leavers_nbr AS leavers_nbr," +
                        "t1.remainings_nbr AS remainings_nbr," +
                        "t1.remainings_prod0 AS remainings_prod0," +
                        "t1.remainings_coors45 AS remainings_coors45," +
                        "t1.remainings_art_comm1 AS remainings_art_comm1," +
                        "t1.remainings_user_comm3 AS remainings_user_comm3," +
                        "t1.superficial_leavers_cnt AS superficial_leavers_cnt," +
                        "t1.substantive_leavers_cnt AS substantive_leavers_cnt," +
                        "t1.wp_tenure AS wp_tenure," +
                        "t1.avg_substantive_leaver_tenure AS avg_substantive_leaver_tenure," +
                        "t1.project_prod0 AS project_prod0," +
                        "IFNULL(t2.project_coors, 0) AS project_coors45," +
                        "FROM " + util.tableName(defaultDataset, "script_results_wp_ivs_cvs_group_ns0"+timeIntervalUnit+"edit"+monthlyEditingThreshold, "t1") +
                        "LEFT JOIN " + util.tableName(defaultDataset, "dv_wp_full_coor45_per_time_interval_months" + timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount",
                TableId.of(defaultDataset, "script_results_wp_ivs_cvs_group_ns045"+timeIntervalUnit+"edit"+monthlyEditingThreshold));


        // merge with group level article communication
        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "t1.newcomers_nbr AS newcomers_nbr," +
                        "t1.leavers_nbr AS leavers_nbr," +
                        "t1.remainings_nbr AS remainings_nbr," +
                        "t1.remainings_prod0 AS remainings_prod0," +
                        "t1.remainings_coors45 AS remainings_coors45," +
                        "t1.remainings_art_comm1 AS remainings_art_comm1," +
                        "t1.remainings_user_comm3 AS remainings_user_comm3," +
                        "t1.superficial_leavers_cnt AS superficial_leavers_cnt," +
                        "t1.substantive_leavers_cnt AS substantive_leavers_cnt," +
                        "t1.wp_tenure AS wp_tenure," +
                        "t1.avg_substantive_leaver_tenure AS avg_substantive_leaver_tenure," +
                        "t1.project_prod0 AS project_prod0," +
                        "t1.project_coors45 AS project_coors45," +
                        "IFNULL(t2.project_art_comm, 0) AS project_art_comm1," +
                        "FROM " + util.tableName(defaultDataset, "script_results_wp_ivs_cvs_group_ns045"+timeIntervalUnit+"edit"+monthlyEditingThreshold, "t1") +
                        "LEFT JOIN " + util.tableName(defaultDataset, "dv_wp_full_art_comm1_per_time_interval_months" + timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount",
                TableId.of(defaultDataset, "script_results_wp_ivs_cvs_group_ns0145"+timeIntervalUnit+"edit"+monthlyEditingThreshold));

        // merge with group level user communication
        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "t1.newcomers_nbr AS newcomers_nbr," +
                        "t1.leavers_nbr AS leavers_nbr," +
                        "t1.remainings_nbr AS remainings_nbr," +
                        "t1.remainings_prod0 AS remainings_prod0," +
                        "t1.remainings_coors45 AS remainings_coors45," +
                        "t1.remainings_art_comm1 AS remainings_art_comm1," +
                        "t1.remainings_user_comm3 AS remainings_user_comm3," +
                        "t1.superficial_leavers_cnt AS superficial_leavers_cnt," +
                        "t1.substantive_leavers_cnt AS substantive_leavers_cnt," +
                        "t1.wp_tenure AS wp_tenure," +
                        "t1.avg_substantive_leaver_tenure AS avg_substantive_leaver_tenure," +
                        "t1.project_prod0 AS project_prod0," +
                        "t1.project_coors45 AS project_coors45," +
                        "t1.project_art_comm1 AS project_art_comm1," +
                        "IFNULL(t2.project_user_comm, 0) AS project_user_comm3," +
                        "FROM " + util.tableName(defaultDataset, "script_results_wp_ivs_cvs_group_ns0145"+timeIntervalUnit+"edit"+monthlyEditingThreshold, "t1") +
                        "LEFT JOIN " + util.tableName(defaultDataset, "dv_wp_full_user_comm3_per_time_interval_months" + timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount " +
                        "ORDER BY nwikiproject, tcount",
                TableId.of(defaultDataset, "script_results_wp_ivs_cvs_group_ns01345"+timeIntervalUnit+"edit"+monthlyEditingThreshold));

        // merge with group coordination 4
        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "t1.newcomers_nbr AS newcomers_nbr," +
                        "t1.leavers_nbr AS leavers_nbr," +
                        "t1.remainings_nbr AS remainings_nbr," +
                        "t1.remainings_prod0 AS remainings_prod0," +
                        "t1.remainings_coors45 AS remainings_coors45," +
                        "t1.remainings_art_comm1 AS remainings_art_comm1," +
                        "t1.remainings_user_comm3 AS remainings_user_comm3," +
                        "t1.superficial_leavers_cnt AS superficial_leavers_cnt," +
                        "t1.substantive_leavers_cnt AS substantive_leavers_cnt," +
                        "t1.wp_tenure AS wp_tenure," +
                        "t1.avg_substantive_leaver_tenure AS avg_substantive_leaver_tenure," +
                        "t1.project_prod0 AS project_prod0," +
                        "IFNULL(t2.project_coor4, 0) AS project_coor4," +
                        "t1.project_coors45 AS project_coors45," +
                        "t1.project_art_comm1 AS project_art_comm1," +
                        "t1.project_user_comm3 AS project_user_comm3," +
                        "FROM " + util.tableName(defaultDataset, "script_results_wp_ivs_cvs_group_ns01345"+timeIntervalUnit+"edit"+monthlyEditingThreshold, "t1") +
                        "LEFT JOIN " + util.tableName(defaultDataset, "dv_wp_full_coor4_per_time_interval_months" + timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount " +
                        "ORDER BY nwikiproject, tcount",
                TableId.of(defaultDataset, "script_results_wp_ivs_cvs_group_ns01345_4"+timeIntervalUnit+"edit"+monthlyEditingThreshold));

        // merge with group coordination 5
        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "t1.newcomers_nbr AS newcomers_nbr," +
                        "t1.leavers_nbr AS leavers_nbr," +
                        "t1.remainings_nbr AS remainings_nbr," +
                        "t1.remainings_prod0 AS remainings_prod0," +
                        "t1.remainings_coors45 AS remainings_coors45," +
                        "t1.remainings_art_comm1 AS remainings_art_comm1," +
                        "t1.remainings_user_comm3 AS remainings_user_comm3," +
                        "t1.superficial_leavers_cnt AS superficial_leavers_cnt," +
                        "t1.substantive_leavers_cnt AS substantive_leavers_cnt," +
                        "t1.wp_tenure AS wp_tenure," +
                        "t1.avg_substantive_leaver_tenure AS avg_substantive_leaver_tenure," +
                        "t1.project_prod0 AS project_prod0," +
                        "t1.project_coor4 AS project_coor4," +
                        "IFNULL(t2.project_coor5, 0) AS project_coor5," +
                        "t1.project_coors45 AS project_coors45," +
                        "t1.project_art_comm1 AS project_art_comm1," +
                        "t1.project_user_comm3 AS project_user_comm3," +
                        "FROM " + util.tableName(defaultDataset, "script_results_wp_ivs_cvs_group_ns01345_4"+timeIntervalUnit+"edit"+monthlyEditingThreshold, "t1") +
                        "LEFT JOIN " + util.tableName(defaultDataset, "dv_wp_full_coor5_per_time_interval_months" + timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount " +
                        "ORDER BY nwikiproject, tcount",
                TableId.of(defaultDataset, "script_results_wp_ivs_cvs_group_ns01345_45"+timeIntervalUnit+"edit"+monthlyEditingThreshold));

        // TODO: can be changed here - select valid wikiprojects with threshold by another table
        // merge with group coordination 5
        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t2.wikiproject AS wikiproject," +
                        "t1.tcount AS tcount," +
                        "t1.newcomers_nbr AS newcomers_nbr," +
                        "t1.leavers_nbr AS leavers_nbr," +
                        "t1.remainings_nbr AS remainings_nbr," +
                        "t1.remainings_prod0 AS remainings_prod0," +
                        "t1.remainings_coors45 AS remainings_coors45," +
                        "t1.remainings_art_comm1 AS remainings_art_comm1," +
                        "t1.remainings_user_comm3 AS remainings_user_comm3," +
                        "t1.superficial_leavers_cnt AS superficial_leavers_cnt," +
                        "t1.substantive_leavers_cnt AS substantive_leavers_cnt," +
                        "t1.wp_tenure AS wp_tenure," +
                        "t1.avg_substantive_leaver_tenure AS avg_substantive_leaver_tenure," +
                        "t1.project_prod0 AS project_prod0," +
                        "t1.project_coor4 AS project_coor4," +
                        "t1.project_coor5 AS project_coor5," +
                        "t1.project_coors45 AS project_coors45," +
                        "t1.project_art_comm1 AS project_art_comm1," +
                        "t1.project_user_comm3 AS project_user_comm3," +
                        "FROM " + util.tableName(defaultDataset, "script_results_wp_ivs_cvs_group_ns01345_45"+timeIntervalUnit+"edit"+monthlyEditingThreshold, "t1") +
                        "INNER JOIN " + util.tableName(defaultDataset, "wp_full_valid_tcounts_above0", "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject " +
                        "ORDER BY nwikiproject, tcount",
                TableId.of(defaultDataset, "script_results_wp_ivs_cvs_group_ns01345_45_abv0"+timeIntervalUnit+"edit"+monthlyEditingThreshold));
    }

    private void computeAdvancedMemberAttributes() throws Exception {
        distinguishProjectLeaverAttributes();
        computeHighLowPerformanceMembers();
        mergeProjectLeaverAttributes();
    }

    private void computeHighLowPerformanceMembers() throws Exception {
        // given table: nwikiproject, tcount, mean, high_bar, low_bar

        //todo: update table names



    }

    private void addAverageLeaverStayingsToResults() throws Exception {
        // script_leaver_wp_project_ns01345
        // TableId.of(defaultDataset, "script_mbr_comp_dv_prod_coors_art_comm_user_comm_leavers_high_low_prod_coors"+timeIntervalUnit))

        // TableId.of(defaultDataset, "script_leaver_wp_leaving_tenure"+timeIntervalUnit+"edit"+monthlyEditingThreshold))
        // Compute average levear tenure per tcount per wikiproject
        util.runQuery("SELECT nwikiproject," +
                        "leaving_tcount AS tcount," +
                        "AVG(leaver_tenure) AS avg_substantive_leaver_tenure," +
                        "FROM " + util.tableName(defaultDataset, "script_leaver_wp_leaving_tenure" + timeIntervalUnit+"edit"+monthlyEditingThreshold) +
                        "WHERE leaver_tenure != 1 " +
                        "GROUP BY nwikiproject, tcount",
                TableId.of(defaultDataset, "script_wp_tcount_avg_leaver_tenure"+timeIntervalUnit+"edit"+monthlyEditingThreshold));

        // Merge with result table
        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "t1.newcomers_nbr AS newcomers_nbr," +
                        "t1.leavers_nbr AS leavers_nbr," +
                        "t1.remainings_nbr AS remainings_nbr," +
                        "t1.remainings_prod0 AS remainings_prod0," +
                        "t1.remainings_coors45 AS remainings_coors45," +
                        "t1.remainings_art_comm1 AS remainings_art_comm1," +
                        "t1.remainings_user_comm3 AS remainings_user_comm3," +
                        "t1.superficial_leavers_cnt AS superficial_leavers_cnt," +
                        "t1.substantive_leavers_cnt AS substantive_leavers_cnt," +
                        "t1.wp_tenure AS wp_tenure," +
                        "IFNULL(t2.avg_substantive_leaver_tenure, 0) AS avg_substantive_leaver_tenure," +
                        "FROM " + util.tableName(defaultDataset, "script_results_wp_ivs_cv"+timeIntervalUnit+"edit"+monthlyEditingThreshold, "t1") +
                        "LEFT JOIN " + util.tableName(defaultDataset, "script_wp_tcount_avg_leaver_tenure"+timeIntervalUnit+"edit"+monthlyEditingThreshold, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount",
                TableId.of(defaultDataset, "script_results_wp_ivs_cvs"+timeIntervalUnit+"edit"+monthlyEditingThreshold));
    }

    private void analysisOnLeavingStayings() throws Exception {
        // compute the enture at the time the leavers left the project
        // group by user, wp
        util.runQuery("SELECT user_id," +
                        "nwikiproject," +
                        "AVG(first_tcount) AS first_tcount," +
                        "AVG(leaving_tcount) AS leaving_tcount," +
                        "FROM " + util.tableName(defaultDataset, "script_user_wp_active_range_revs45" + timeIntervalUnit + "edit" + monthlyEditingThreshold) +
                        "GROUP BY user_id, nwikiproject",
                TableId.of(defaultDataset, "script_user_wp_joining_leaving_tcount"+timeIntervalUnit+"edit"+monthlyEditingThreshold));

        // join with project productivity, etc at leaving tcount
        util.runQuery("SELECT user_id, " +
                        "nwikiproject," +
                        "INTEGER(first_tcount) AS first_tcount," +
                        "INTEGER(leaving_tcount) AS leaving_tcount," +
                        "INTEGER(leaving_tcount-first_tcount) AS leaver_tenure," +
                        "FROM " + util.tableName(defaultDataset, "script_user_wp_joining_leaving_tcount"+timeIntervalUnit+"edit"+monthlyEditingThreshold),
                TableId.of(defaultDataset, "script_leaver_wp_leaving_tenure"+timeIntervalUnit+"edit"+monthlyEditingThreshold));

        // join with the project performance at the time leavers left
        // project productivity
        util.runQuery("SELECT t1.user_id AS user_id," +
                        "t1.nwikiproject AS nwikiproject," +
                        "t1.leaving_tcount AS leaving_tcount," +
                        "t1.leaver_tenure AS leaver_tenure," +
                        "t2.aggr_prod0 AS remaining_prod0," +
                        "FROM " + util.tableName(defaultDataset, "script_leaver_wp_leaving_tenure" + timeIntervalUnit + "edit" + monthlyEditingThreshold, "t1") +
                        "INNER JOIN " + util.tableName(defaultDataset, "script_wp_remainings_aggr_productivity_ts" + timeIntervalUnit + "edit" + monthlyEditingThreshold, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.leaving_tcount = t2.tcount",
                TableId.of(defaultDataset, "script_leaver_wp_project_ns0" + timeIntervalUnit + "edit"+monthlyEditingThreshold));

        // project coordination
        util.runQuery("SELECT t1.user_id AS user_id," +
                        "t1.nwikiproject AS nwikiproject," +
                        "t1.leaving_tcount AS leaving_tcount," +
                        "t1.leaver_tenure AS leaver_tenure," +
                        "t1.remaining_prod0 AS remainings_prod0," +
                        "t2.aggr_coors45 AS remainings_coors45," +
                        "FROM " + util.tableName(defaultDataset, "script_leaver_wp_project_ns0" + timeIntervalUnit + "edit" + monthlyEditingThreshold, "t1") +
                        "INNER JOIN " + util.tableName(defaultDataset, "script_wp_remainings_aggr_coordination45_ts" + timeIntervalUnit+"edit"+monthlyEditingThreshold, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.leaving_tcount = t2.tcount",
                TableId.of(defaultDataset, "script_leaver_wp_project_ns045" + timeIntervalUnit + "edit"+monthlyEditingThreshold));

        // article communication
        util.runQuery("SELECT t1.user_id AS user_id," +
                        "t1.nwikiproject AS nwikiproject," +
                        "t1.leaving_tcount AS leaving_tcount," +
                        "t1.leaver_tenure AS leaver_tenure," +
                        "t1.remainings_prod0 AS remainings_prod0," +
                        "t1.remainings_coors45 AS remainings_coors45," +
                        "t2.aggr_art_comm1 AS remainings_art_comm1," +
                        "FROM " + util.tableName(defaultDataset, "script_leaver_wp_project_ns045" + timeIntervalUnit + "edit" + monthlyEditingThreshold, "t1") +
                        "INNER JOIN " + util.tableName(defaultDataset, "script_wp_remainings_aggr_art_comm1_ts" + timeIntervalUnit + "edit" + monthlyEditingThreshold, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.leaving_tcount = t2.tcount",
                TableId.of(defaultDataset, "script_leaver_wp_project_ns0145" + timeIntervalUnit + "edit"+monthlyEditingThreshold));

        // user communication
        util.runQuery("SELECT t1.user_id AS user_id," +
                        "t1.nwikiproject AS nwikiproject," +
                        "t1.leaving_tcount AS leaving_tcount," +
                        "t1.leaver_tenure AS leaver_tenure," +
                        "t1.remainings_prod0 AS remainings_prod0," +
                        "t1.remainings_coors45 AS remainings_coors45," +
                        "t1.remainings_art_comm1 AS remainings_art_comm1," +
                        "t2.aggr_user_comm3 AS remainings_user_comm3," +
                        "FROM " + util.tableName(defaultDataset, "script_leaver_wp_project_ns0145" + timeIntervalUnit + "edit" + monthlyEditingThreshold, "t1") +
                        "INNER JOIN " + util.tableName(defaultDataset, "script_wp_remainings_aggr_user_comm3_ts" + timeIntervalUnit + "edit" + monthlyEditingThreshold, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.leaving_tcount = t2.tcount",
                TableId.of(defaultDataset, "script_leaver_wp_project_ns01345" + timeIntervalUnit + "edit"+monthlyEditingThreshold));
    }

    /**
     * function operations:
     * 1. compute the composition of editors using operationalizations with edits on articles.
     *      1.1 number of newcomers/leavers/remainings
     *      1.2 superficial & substantive leavers
     * 2. compute remaining members project performance.
     * 3. compute CVs
     * 4. merge the variables
     *
     * @throws Exception
     */
    private void relocateEditorActiveRangeOnProjects() throws Exception {
        /** table to change: script_user_wp_active_range_revs45, function to change - identifyMembersInProjects(); */

        articleActivityInvolvedMemberActiveRange();

        updatedRemainingsProjectPerformance();
        mergingNewEditorMemberships();

        mergeCVAndNewEditorMemberships();
    }

    private void articleActivityInvolvedMemberActiveRange() throws Exception {
        // Locate the first edit to a time interval of an editor to a project
        util.runQuery("SELECT t1.user_id AS user_id," +
                        "t1.nwikiproject AS nwikiproject," +
                        "t1.first_edit AS first_edit," +
                        "t2.tcount AS first_tcount," +
                        "t1.last_edit AS last_edit," +
                        "FROM " + util.tableName("bowen_wikis_quitting", "rev_ns45_user_wikiproject_ts_range_encoded", "t1") +
                        "INNER JOIN " + util.tableName(defaultDataset, "script_user_wp_revs_45_valid_users_wps_valid_range" + timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject " +
                        "WHERE t2.start_ts <= t1.first_edit AND t1.first_edit < t2.end_ts ",
                TableId.of(defaultDataset, "script_user_wp_active_start_end_revs45_partial"+timeIntervalUnit));

        // Locate the last edit for the editor on each project involved
        util.runQuery("SELECT t1.user_id AS user_id," +
                        "t1.nwikiproject AS nwikiproject," +
                        "t1.first_tcount AS first_tcount," +
                        "t1.last_edit AS last_edit," +
                        "t2.tcount AS last_tcount," +
                        "FROM " + util.tableName(defaultDataset, "script_user_wp_active_start_end_revs45_partial" + timeIntervalUnit, "t1") +
                        "INNER JOIN " + util.tableName(defaultDataset, "script_user_wp_revs_45_valid_users_wps_valid_range" + timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject " +
                        "WHERE t2.start_ts <= t1.last_edit AND t1.last_edit < t2.end_ts",
                TableId.of(defaultDataset, "script_user_wp_active_start_end_revs45"+timeIntervalUnit));

        // editor's edits on articles within the scope.
        // ns table: rev_ns0_valid_user_wikiproject_std
        // Q: how to segement the edits by intervals? - check project's tcount for segementation
        // table to use (group level): script_user_wp_revs_45_valid_users_wps_valid_range1

        // aggregate the number of edits the edit made on the articles within the scope of the project - need to check if editor joined the project
        util.runQuery("SELECT t1.user_id AS user_id," +
                        "t1.nwikiproject AS nwikiproject," +
                        "t2.tcount AS tcount," +
                        "COUNT(*) AS article_edits," +
                        "FROM " + util.tableName("bowen_user_dropouts", "rev_ns0_valid_user_wikiproject_std", "t1") +
                        "INNER JOIN " + util.tableName(defaultDataset, "script_user_wp_revs_45_valid_users_wps_valid_range"+timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject " +
                        "WHERE t1.timestamp >= t2.start_ts AND t1.timestamp < t2.end_ts " +
                        "GROUP BY user_id, nwikiproject, tcount",
                TableId.of(defaultDataset, "script_editor_wp_tcount_ns0_edits"+timeIntervalUnit));

        // join with the project joining table for the starting time
        // append editor's first tcount on wikiprojects to the article edits per tcount
        util.runQuery("SELECT t1.user_id AS user_id," +
                        "t1.nwikiproject AS nwikiproject," +
                        "t1.first_tcount AS first_tcount," +
                        "t2.tcount AS tcount," +
                        "t2.article_edits AS article_edits," +
                        "FROM " + util.tableName(defaultDataset, "script_user_wp_active_start_end_revs45_partial"+timeIntervalUnit, "t1") +
                        "INNER JOIN " + util.tableName(defaultDataset, "script_editor_wp_tcount_ns0_edits"+timeIntervalUnit, "t2") +
                        "ON t1.user_id = t2.user_id AND t1.nwikiproject = t2.nwikiproject " +
                        "WHERE t1.first_tcount <= t2.tcount",
                TableId.of(defaultDataset, "script_editor_wp_first_tcount_and_article_edits_per_tcount"+timeIntervalUnit));

        // find the first tcount that has less than K edits as the sign of leaving the project TODO: null?
        util.runQuery("SELECT user_id," +
                        "nwikiproject," +
                        "MIN(tcount) AS article_last_tcount," +
                        "FROM " + util.tableName(defaultDataset, "script_editor_wp_first_tcount_and_article_edits_per_tcount"+timeIntervalUnit) +
                        "WHERE article_edits < " + monthlyEditingThreshold + " AND tcount >= first_tcount " +
                        "GROUP BY user_id, nwikiproject",
                TableId.of(defaultDataset, "script_editor_wp_valid_ns0_quitting_tcount"+timeIntervalUnit+"edits"+monthlyEditingThreshold));


        // find article quitting tcount and the project page quitting tcount whichever comes the last //TODO join type?
        // join the two tables with two exiting computations
        util.runQuery("SELECT t1.user_id AS user_id," +
                        "t1.nwikiproject AS nwikiproject," +
                        "t2.first_tcount AS project_first_tcount," +
                        "IFNULL(t1.article_last_tcount, t2.last_edit) AS article_last_tcount," +
                        "t2.last_tcount AS project_last_tcount," +
                        "FROM " + util.tableName(defaultDataset, "script_editor_wp_valid_ns0_quitting_tcount" + timeIntervalUnit + "edits" + monthlyEditingThreshold, "t1") +
                        "LEFT JOIN " + util.tableName(defaultDataset, "script_user_wp_active_start_end_revs45" + timeIntervalUnit, "t2") +
                        "ON t1.user_id = t2.user_id AND t1.nwikiproject = t2.nwikiproject",
                TableId.of(defaultDataset, "script_user_wp_first_last_tcounts_partial"+timeIntervalUnit+"edits"+monthlyEditingThreshold));

        // TODO: key choice - select article last tcount and project tcount whichever comes last...
        util.runQuery("SELECT user_id," +
                        "nwikiproject," +
                        "project_first_tcount AS first_tcount," +
                        "IF(project_last_tcount > article_last_tcount, project_last_tcount, article_last_tcount) AS last_tcount," +
                        "FROM " + util.tableName(defaultDataset, "script_user_wp_first_last_tcounts_partial"+timeIntervalUnit+"edits"+monthlyEditingThreshold),
                TableId.of(defaultDataset, "script_user_wp_first_last_tcounts"+timeIntervalUnit+"edits"+monthlyEditingThreshold));

        ///********************* todo: better design of naming of two parameters

        // Create the entire active range of the editor on the project by filling the gaps
        util.runQuery("SELECT t1.user_id AS user_id," +
                        "t1.nwikiproject AS nwikiproject," +
                        "t2.tcount AS tcount," +
                        "t1.first_tcount AS first_tcount," +
                        "t1.last_tcount AS last_tcount," +
                        "(t1.last_tcount+1) AS leaving_tcount," +
                        "FROM " + util.tableName(defaultDataset, "script_user_wp_first_last_tcounts"+timeIntervalUnit+"edits"+monthlyEditingThreshold, "t1") +
                        "INNER JOIN " + util.tableName(defaultDataset, "script_user_wp_revs_45_valid_users_wps_valid_range" + timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject " +
                        "WHERE t1.first_tcount <= t2.tcount AND t1.last_tcount >= t2.tcount ",
//                        "ORDER BY nwikiproject, user_id, tcount", // computation power limitation
                TableId.of(defaultDataset, "script_user_wp_active_range_revs45"+timeIntervalUnit+"edit"+monthlyEditingThreshold));

        // Compute newcomers for each project in each time interval
        util.runQuery("SELECT nwikiproject," +
                        "tcount," +
                        "COUNT(UNIQUE(user_id)) AS newcomers_nbr," +
                        "FROM " + util.tableName(defaultDataset, "script_user_wp_active_range_revs45" + timeIntervalUnit + "edit" + monthlyEditingThreshold) +
                        "WHERE tcount = first_tcount " +
                        "GROUP BY nwikiproject, tcount",
                TableId.of(defaultDataset, "script_wp_newcomers_nbr_tcount" + timeIntervalUnit+"edit"+monthlyEditingThreshold));

        // add in time intervals with no such members
        // TODO: do not count the first and last time intervals - to remove (identify the value)
        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "IFNULL(t2.newcomers_nbr, 0) AS newcomers_nbr," +
                        "FROM " + util.tableName(defaultDataset, "script_user_wp_revs_45_valid_users_wps_valid_range" + timeIntervalUnit, "t1") +
                        "LEFT JOIN " + util.tableName(defaultDataset, "script_wp_newcomers_nbr_tcount"+timeIntervalUnit+"edit"+monthlyEditingThreshold, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount " +
                        "WHERE t1.tcount != 0",
                TableId.of(defaultDataset, "script_wp_newcomers_nbr_tcount_full"+timeIntervalUnit+"edit"+monthlyEditingThreshold));

        // leavers
        util.runQuery("SELECT nwikiproject," +
                        "(tcount+1) AS tcount," +
                        "COUNT(UNIQUE(user_id)) AS leavers_nbr," +
                        "FROM " + util.tableName(defaultDataset, "script_user_wp_active_range_revs45" + timeIntervalUnit + "edit" + monthlyEditingThreshold) +
                        "WHERE (tcount+1) = leaving_tcount " +
                        "GROUP BY nwikiproject, tcount",
                TableId.of(defaultDataset, "script_wp_leavers_nbr_tcount"+timeIntervalUnit+"edit"+monthlyEditingThreshold));

        // superficial leavers
        util.runQuery("SELECT nwikiproject," +
                        "(tcount+1) AS tcount," +
                        "COUNT(UNIQUE(user_id)) AS superficial_leavers_cnt," +
                        "FROM " + util.tableName(defaultDataset, "script_user_wp_active_range_revs45" + timeIntervalUnit + "edit" + monthlyEditingThreshold) +
                        "WHERE tcount = first_tcount AND (tcount+1) = leaving_tcount " +
                        "GROUP BY nwikiproject, tcount",
                TableId.of(defaultDataset, "script_wp_superficial_leavers_nbr_tcount"+timeIntervalUnit+"edit"+monthlyEditingThreshold));

        // substantive leavers
        util.runQuery("SELECT nwikiproject," +
                        "(tcount+1) AS tcount," +
                        "COUNT(UNIQUE(user_id)) AS substantive_leavers_cnt," +
                        "FROM " + util.tableName(defaultDataset, "script_user_wp_active_range_revs45" + timeIntervalUnit + "edit" + monthlyEditingThreshold) +
                        "WHERE tcount != first_tcount AND (tcount+1) = leaving_tcount " +
                        "GROUP BY nwikiproject, tcount",
                TableId.of(defaultDataset, "script_wp_substantive_leavers_nbr_tcount"+timeIntervalUnit+"edit"+monthlyEditingThreshold));

        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "IFNULL(t2.leavers_nbr, 0) AS leavers_nbr," +
                        "FROM " + util.tableName(defaultDataset, "script_user_wp_revs_45_valid_users_wps_valid_range" + timeIntervalUnit, "t1") +
                        "LEFT JOIN " + util.tableName(defaultDataset, "script_wp_leavers_nbr_tcount"+timeIntervalUnit+"edit"+monthlyEditingThreshold, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount " +
                        "WHERE t1.tcount != 0", // TODO check the last time interval value
                TableId.of(defaultDataset, "script_wp_leavers_nbr_tcount_full"+timeIntervalUnit+"edit"+monthlyEditingThreshold));

        // remaining members
        util.runQuery("SELECT nwikiproject," +
                        "tcount," +
                        "COUNT(UNIQUE(user_id)) AS remainings_nbr," +
                        "FROM " + util.tableName(defaultDataset, "script_user_wp_active_range_revs45" + timeIntervalUnit + "edit" + monthlyEditingThreshold) +
                        "WHERE tcount != first_tcount " +
                        "GROUP BY nwikiproject, tcount",
                TableId.of(defaultDataset, "script_wp_remainings_nbr_tcount"+timeIntervalUnit+"edit"+monthlyEditingThreshold));

        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "IFNULL(t2.remainings_nbr, 0) AS remainings_nbr," +
                        "FROM " + util.tableName(defaultDataset, "script_user_wp_revs_45_valid_users_wps_valid_range"+timeIntervalUnit, "t1") +
                        "LEFT JOIN " + util.tableName(defaultDataset, "script_wp_remainings_nbr_tcount" + timeIntervalUnit+"edit"+monthlyEditingThreshold, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount " +
                        "WHERE t1.tcount != 0", // TODO check the last time interval value
                TableId.of(defaultDataset, "script_wp_remainings_nbr_tcount_full"+timeIntervalUnit+"edit"+monthlyEditingThreshold));

    }


    // this function uses the editors using the new defined active time range
    private void updatedRemainingsProjectPerformance() throws Exception {

        // select remaining members to get (user-wp-trange) table
        util.runQuery("SELECT *," +
                        "FROM " + util.tableName(defaultDataset, "script_user_wp_active_range_revs45" + timeIntervalUnit + "edit" + monthlyEditingThreshold) +
                        "WHERE tcount != first_tcount ",
                TableId.of(defaultDataset, "script_wp_remainings_edit_tcount"+timeIntervalUnit+"edit"+monthlyEditingThreshold));

        // append the timestamp for each project
        // script_user_wp_revs_45_valid_users_wps_valid_range1
        util.runQuery("SELECT t1.user_id AS user_id," +
                        "t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "t2.start_ts AS start_ts," +
                        "t2.end_ts AS end_ts," +
                        "t1.first_tcount AS first_tcount," +
                        "t1.last_tcount AS last_tcount," +
                        "t1.leaving_tcount AS leaving_tcount," +
                        "FROM " + util.tableName(defaultDataset, "script_wp_remainings_edit_tcount"+timeIntervalUnit+"edit"+monthlyEditingThreshold, "t1") +
                        "INNER JOIN " + util.tableName(defaultDataset, "script_user_wp_revs_45_valid_users_wps_valid_range"+timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount",
                TableId.of(defaultDataset, "script_wp_remainings_edit_tcount_range_ts"+timeIntervalUnit+"edit"+monthlyEditingThreshold));

        // join with rev ns 0 for project level productivity
        util.runQuery("SELECT t1.user_id AS user_id," +
                        "t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "COUNT(*) AS aggr_prod0," +
                        "FROM " + util.tableName(defaultDataset, "script_wp_remainings_edit_tcount_range_ts" + timeIntervalUnit + "edit" + monthlyEditingThreshold, "t1") +
                        "INNER JOIN " + util.tableName("bowen_user_dropouts", "rev_ns0_valid_user_wikiproject_std", "t2") +
                        "ON t1.user_id = t2.user_id AND t1.nwikiproject = t2.nwikiproject " +
                        "WHERE t2.timestamp < t1.end_ts AND t2.timestamp >= t1.start_ts " +
                        "GROUP BY user_id, nwikiproject, tcount",
                TableId.of(defaultDataset, "script_wp_remainings_productivity_ts"+timeIntervalUnit+"edit"+monthlyEditingThreshold));

        util.runQuery("SELECT nwikiproject," +
                        "tcount," +
                        "SUM(aggr_prod0) AS aggr_prod0," +
                        "FROM " + util.tableName(defaultDataset, "script_wp_remainings_productivity_ts" + timeIntervalUnit + "edit" + monthlyEditingThreshold) +
                        "GROUP BY nwikiproject, tcount",
                TableId.of(defaultDataset, "script_wp_remainings_aggr_productivity_ts"+timeIntervalUnit+"edit"+monthlyEditingThreshold));

        // join with rev ns 45 for project level coordination
        util.runQuery("SELECT t1.user_id AS user_id," +
                        "t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "COUNT(*) AS aggr_coors45," +
                        "FROM " + util.tableName(defaultDataset, "script_wp_remainings_edit_tcount_range_ts" + timeIntervalUnit + "edit" + monthlyEditingThreshold, "t1") +
                        "INNER JOIN " + util.tableName("bowen_wikis_quitting", "lng_user_wikiproject_valid_revs_45", "t2") +
                        "ON t1.user_id = t2.user_id AND t1.nwikiproject = t2.nwikiproject " +
                        "WHERE t2.timestamp < t1.end_ts AND t2.timestamp >= t1.start_ts " +
                        "GROUP BY user_id, nwikiproject, tcount",
                TableId.of(defaultDataset, "script_wp_remainings_coordination45_ts"+timeIntervalUnit+"edit"+monthlyEditingThreshold));

        util.runQuery("SELECT nwikiproject," +
                        "tcount," +
                        "SUM(aggr_coors45) AS aggr_coors45," +
                        "FROM " + util.tableName(defaultDataset, "script_wp_remainings_coordination45_ts" + timeIntervalUnit + "edit" + monthlyEditingThreshold) +
                        "GROUP BY nwikiproject, tcount",
                TableId.of(defaultDataset, "script_wp_remainings_aggr_coordination45_ts" + timeIntervalUnit+"edit"+monthlyEditingThreshold));

        // article communication ns1
        util.runQuery("SELECT t1.user_id AS user_id," +
                        "t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "COUNT(*) AS aggr_art_comm1," +
                        "FROM " + util.tableName(defaultDataset, "script_wp_remainings_edit_tcount_range_ts" + timeIntervalUnit + "edit" + monthlyEditingThreshold, "t1") +
                        "INNER JOIN " + util.tableName("bowen_wikis_quitting", "valid_user_wikiproject_article_talk_pages_std", "t2") +
                        "ON t1.user_id = t2.user_id AND t1.nwikiproject = t2.nwikiproject " +
                        "WHERE t2.timestamp < t1.end_ts AND t2.timestamp >= t1.start_ts " +
                        "GROUP BY user_id, nwikiproject, tcount",
                TableId.of(defaultDataset, "script_wp_remainings_art_comm1_ts"+timeIntervalUnit+"edit"+monthlyEditingThreshold));

        util.runQuery("SELECT nwikiproject," +
                        "tcount," +
                        "SUM(aggr_art_comm1) AS aggr_art_comm1," +
                        "FROM " + util.tableName(defaultDataset, "script_wp_remainings_art_comm1_ts" + timeIntervalUnit + "edit" + monthlyEditingThreshold) +
                        "GROUP BY nwikiproject, tcount",
                TableId.of(defaultDataset, "script_wp_remainings_aggr_art_comm1_ts"+timeIntervalUnit+"edit"+monthlyEditingThreshold));


        // member user table ns 3
        // where was the table? was confused to user article communication...
        //lng_user_wikiproject_valid_revs_3s
        util.runQuery("SELECT t1.user_id AS user_id," +
                        "t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "COUNT(*) AS aggr_user_comm3," +
                        "FROM " + util.tableName(defaultDataset, "script_wp_remainings_edit_tcount_range_ts" + timeIntervalUnit + "edit" + monthlyEditingThreshold, "t1") +
                        "INNER JOIN " + util.tableName("bowen_user_dropouts", "lng_user_wikiproject_valid_revs_3s", "t2") +
                        "ON t1.user_id = t2.user_id AND t1.nwikiproject = t2.nwikiproject " +
                        "WHERE t2.timestamp < t1.end_ts AND t2.timestamp >= t1.start_ts " +
                        "GROUP BY user_id, nwikiproject, tcount",
                TableId.of(defaultDataset, "script_wp_remainings_user_comm3_ts"+timeIntervalUnit+"edit"+monthlyEditingThreshold));

        util.runQuery("SELECT nwikiproject," +
                        "tcount," +
                        "SUM(aggr_user_comm3) AS aggr_user_comm3," +
                        "FROM " + util.tableName(defaultDataset, "script_wp_remainings_user_comm3_ts" + timeIntervalUnit + "edit" + monthlyEditingThreshold) +
                        "GROUP BY nwikiproject, tcount",
                TableId.of(defaultDataset, "script_wp_remainings_aggr_user_comm3_ts"+timeIntervalUnit+"edit"+monthlyEditingThreshold));

    }


    private void mergingNewEditorMemberships() throws Exception {

        // merge newcomers and leavers
        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "t1.newcomers_nbr AS newcomers_nbr," +
                        "t2.leavers_nbr AS leavers_nbr," +
                        "FROM " + util.tableName(defaultDataset, "script_wp_newcomers_nbr_tcount_full"+timeIntervalUnit+"edit"+monthlyEditingThreshold, "t1") +
                        "INNER JOIN " + util.tableName(defaultDataset, "script_wp_leavers_nbr_tcount_full"+timeIntervalUnit+"edit"+monthlyEditingThreshold, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount",
                TableId.of(defaultDataset, "script_wp_newcomers_leavers_nbr_tcount_full"+timeIntervalUnit+"edit"+monthlyEditingThreshold));

        // merge in the remainings
        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "t1.newcomers_nbr AS newcomers_nbr," +
                        "t1.leavers_nbr AS leavers_nbr," +
                        "t2.remainings_nbr AS remainings_nbr," +
                        "FROM " + util.tableName(defaultDataset, "script_wp_newcomers_leavers_nbr_tcount_full" + timeIntervalUnit + "edit" + monthlyEditingThreshold, "t1") +
                        "INNER JOIN " + util.tableName(defaultDataset, "script_wp_remainings_nbr_tcount_full" + timeIntervalUnit + "edit" + monthlyEditingThreshold, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount",
                TableId.of(defaultDataset, "script_wp_newcomers_leavers_remainings_nbr_tcount_full"+timeIntervalUnit+"edit"+monthlyEditingThreshold));


        // merge project remaining member productivity
        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "t1.newcomers_nbr AS newcomers_nbr," +
                        "t1.leavers_nbr AS leavers_nbr," +
                        "t1.remainings_nbr AS remainings_nbr," +
//                        "t2.aggr_prod0 AS remainings_prod0," +
                        "IFNULL(t2.aggr_prod0, 0) AS remainings_prod0," +
                        "FROM " + util.tableName(defaultDataset, "script_wp_newcomers_leavers_remainings_nbr_tcount_full"+timeIntervalUnit+"edit"+monthlyEditingThreshold, "t1") +
                        "LEFT JOIN " + util.tableName(defaultDataset, "script_wp_remainings_aggr_productivity_ts"+timeIntervalUnit+"edit"+monthlyEditingThreshold, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount",
                TableId.of(defaultDataset, "script_wp_newcomers_leavers_remainings_nbr_prod_tcount_full"+timeIntervalUnit+"edit"+monthlyEditingThreshold));

        // merge project remaining member coordination
        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "t1.newcomers_nbr AS newcomers_nbr," +
                        "t1.leavers_nbr AS leavers_nbr," +
                        "t1.remainings_nbr AS remainings_nbr," +
                        "t1.remainings_prod0 AS remainings_prod0," +
//                        "t2.aggr_coors45 AS remainings_coors45," +
                        "IFNULL(t2.aggr_coors45, 0) AS remainings_coors45," +
                        "FROM " + util.tableName(defaultDataset, "script_wp_newcomers_leavers_remainings_nbr_prod_tcount_full"+timeIntervalUnit+"edit"+monthlyEditingThreshold, "t1") +
                        "LEFT JOIN " + util.tableName(defaultDataset, "script_wp_remainings_aggr_coordination45_ts"+timeIntervalUnit+"edit"+monthlyEditingThreshold, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount",
                TableId.of(defaultDataset, "script_wp_newcomers_leavers_remainings_nbr_prod_coors_tcount_full"+timeIntervalUnit+"edit"+monthlyEditingThreshold));

        // todo : add in remainings edits on article talk, and user talk
        // merge project remaining member article communication
        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "t1.newcomers_nbr AS newcomers_nbr," +
                        "t1.leavers_nbr AS leavers_nbr," +
                        "t1.remainings_nbr AS remainings_nbr," +
                        "t1.remainings_prod0 AS remainings_prod0," +
                        "t1.remainings_coors45 AS remainings_coors45," +
                        "IFNULL(t2.aggr_art_comm1, 0) AS remainings_aggr_art_comm1," +
                        "FROM " + util.tableName(defaultDataset, "script_wp_newcomers_leavers_remainings_nbr_prod_coors_tcount_full"+timeIntervalUnit+"edit"+monthlyEditingThreshold, "t1") +
                        "LEFT JOIN " + util.tableName(defaultDataset, "script_wp_remainings_aggr_art_comm1_ts"+timeIntervalUnit+"edit"+monthlyEditingThreshold, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount",
                TableId.of(defaultDataset, "script_wp_newcomers_leavers_remainings_nbr_prod_coors_ns1_tcount_full"+timeIntervalUnit+"edit"+monthlyEditingThreshold));

        // merge project remaining member user communication
        // defaultDataset, "script_wp_remainings_aggr_user_comm3_ts"+timeIntervalUnit+"edit"+monthlyEditingThreshold
        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "t1.newcomers_nbr AS newcomers_nbr," +
                        "t1.leavers_nbr AS leavers_nbr," +
                        "t1.remainings_nbr AS remainings_nbr," +
                        "t1.remainings_prod0 AS remainings_prod0," +
                        "t1.remainings_coors45 AS remainings_coors45," +
                        "t1.remainings_aggr_art_comm1 AS remainings_art_comm1," +
                        "IFNULL(t2.aggr_user_comm3, 0) AS remainings_user_comm3," +
                        "FROM " + util.tableName(defaultDataset, "script_wp_newcomers_leavers_remainings_nbr_prod_coors_ns1_tcount_full"+timeIntervalUnit+"edit"+monthlyEditingThreshold, "t1") +
                        "LEFT JOIN " + util.tableName(defaultDataset, "script_wp_remainings_aggr_user_comm3_ts"+timeIntervalUnit+"edit"+monthlyEditingThreshold, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount",
                TableId.of(defaultDataset, "script_wp_newcomers_leavers_remainings_nbr_prod_coors_ns13_tcount_full"+timeIntervalUnit+"edit"+monthlyEditingThreshold));

        // merge in superficial leavers
        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "t1.newcomers_nbr AS newcomers_nbr," +
                        "t1.leavers_nbr AS leavers_nbr," +
                        "t1.remainings_nbr AS remainings_nbr," +
                        "t1.remainings_prod0 AS remainings_prod0," +
                        "t1.remainings_coors45 AS remainings_coors45," +
                        "t1.remainings_art_comm1 AS remainings_art_comm1," +
                        "t1.remainings_user_comm3 AS remainings_user_comm3," +
                        "IFNULL(t2.superficial_leavers_cnt, 0) AS superficial_leavers_cnt," +
                        "FROM " + util.tableName(defaultDataset, "script_wp_newcomers_leavers_remainings_nbr_prod_coors_ns13_tcount_full"+timeIntervalUnit+"edit"+monthlyEditingThreshold, "t1") +
                        "LEFT JOIN " + util.tableName(defaultDataset, "script_wp_superficial_leavers_nbr_tcount"+timeIntervalUnit+"edit"+monthlyEditingThreshold, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount",
                TableId.of(defaultDataset, "script_wp_newcomers_sup_leavers_remainings_nbr_prod_coors_ns13_tcount_full"+timeIntervalUnit+"edit"+monthlyEditingThreshold));

        //script_wp_substantive_leavers_nbr_tcount
        // merge substantive leavers
        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "t1.newcomers_nbr AS newcomers_nbr," +
                        "t1.leavers_nbr AS leavers_nbr," +
                        "t1.remainings_nbr AS remainings_nbr," +
                        "t1.remainings_prod0 AS remainings_prod0," +
                        "t1.remainings_coors45 AS remainings_coors45," +
                        "t1.remainings_art_comm1 AS remainings_art_comm1," +
                        "t1.remainings_user_comm3 AS remainings_user_comm3," +
                        "t1.superficial_leavers_cnt AS superficial_leavers_cnt," +
                        "IFNULL(t2.substantive_leavers_cnt, 0) AS substantive_leavers_cnt," +
                        "FROM " + util.tableName(defaultDataset, "script_wp_newcomers_sup_leavers_remainings_nbr_prod_coors_ns13_tcount_full"+timeIntervalUnit+"edit"+monthlyEditingThreshold, "t1") +
                        "LEFT JOIN " + util.tableName(defaultDataset, "script_wp_substantive_leavers_nbr_tcount"+timeIntervalUnit+"edit"+monthlyEditingThreshold, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount",
                TableId.of(defaultDataset, "script_wp_newcomers_sup_sub_leavers_remainings_nbr_prod_coors_ns13_tcount_full"+timeIntervalUnit+"edit"+monthlyEditingThreshold));

    }

    private void mergeProjectLeaverAttributes() throws Exception {
        //TableId.of(defaultDataset, "script_ns0_stdev_bins_high_prod_leavers_nbrs_full" + timeIntervalUnit));
        //TableId.of(defaultDataset, "script_ns0_stdev_bins_low_prod_leavers_nbrs_full" + timeIntervalUnit));

        // merge high/low productive leavers
        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "t1.nbr_high_prod_leavers AS nbr_high_prod_leavers," +
                        "t2.nbr_low_prod_leavers AS nbr_low_prod_leavers," +
                        "FROM " + util.tableName(defaultDataset, "script_ns0_stdev_bins_high_prod_leavers_nbrs_full" + timeIntervalUnit, "t1") +
                        "INNER JOIN " + util.tableName(defaultDataset, "script_ns0_stdev_bins_low_prod_leavers_nbrs_full" + timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount",
                TableId.of(defaultDataset, "script_merging_prod_high_low_nbrs" + timeIntervalUnit));

        // merge high/low project coordination leavers
        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "t1.nbr_high_coors_leavers AS nbr_high_coors_leavers," +
                        "t2.nbr_low_coors_leavers AS nbr_low_coors_leavers," +
                        "FROM " + util.tableName(defaultDataset, "script_ns45_stdev_bins_high_coors_leavers_nbrs_full" + timeIntervalUnit, "t1") +
                        "INNER JOIN " + util.tableName(defaultDataset, "script_ns45_stdev_bins_low_coors_leavers_nbrs_full" + timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount",
                TableId.of(defaultDataset, "script_merging_coors_high_low_nbrs" + timeIntervalUnit));

        // merge the two tables above
        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "t1.nbr_high_prod_leavers AS nbr_high_prod_leavers," +
                        "t1.nbr_low_prod_leavers AS nbr_low_prod_leavers," +
                        "t2.nbr_high_coors_leavers AS nbr_high_coors_leavers," +
                        "t2.nbr_low_coors_leavers AS nbr_low_coors_leavers," +
                        "FROM " + util.tableName(defaultDataset, "script_merging_prod_high_low_nbrs" + timeIntervalUnit, "t1") +
                        "INNER JOIN " + util.tableName(defaultDataset, "script_merging_coors_high_low_nbrs" + timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount",
                TableId.of(defaultDataset, "script_merging_coors_prod_high_low_nbrs" + timeIntervalUnit));

        // merge into previous table
        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "t1.newcomers_nbr AS newcomers_nbr," +
                        "t1.remainings_nbr AS remainings_nbr," +
                        "t1.leavers_nbr AS leavers_nbr," +
                        "t1.group_article_productivity AS group_article_productivity," +
                        "t1.project_coors AS project_coors," +
                        "t1.project_art_comm AS project_art_comm," +
                        "t1.project_user_comm AS project_user_comm," +
                        "IFNULL(t2.nbr_high_prod_leavers, 0) AS nbr_high_prod_leavers," +
                        "IFNULL(t2.nbr_low_prod_leavers, 0) AS nbr_low_prod_leavers," +
                        "IFNULL(t2.nbr_high_coors_leavers, 0) AS nbr_high_coors_leavers," +
                        "IFNULL(t2.nbr_low_coors_leavers, 0) AS nbr_low_coors_leavers," +
                        "FROM " + util.tableName(defaultDataset, "script_mbr_comp_dv_prod_coors_art_comm_user_comm"+timeIntervalUnit, "t1") +
                        "LEFT JOIN " + util.tableName(defaultDataset, "script_merging_coors_prod_high_low_nbrs" + timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount",
                TableId.of(defaultDataset, "script_mbr_comp_dv_prod_coors_art_comm_user_comm_leavers_high_low_prod_coors"+timeIntervalUnit));
    }

    /**
     * Compute high/low performance editors based on standard deviations
     * TODO: document details about IVs generations (what variable and how to generate) based on the existing query document
     * TODO: check what was done, what variables are meaningful to generate (the meaning and rationale of having it)
     */

    private void distinguishProjectLeaverAttributes() throws Exception {

        // todo newcomers - previous experience (# of projects participated before)
        /**
         * Documenting highly productive leaver:
         * For the current time interval t_i, find the productivity in the previous time interval t_(i-1),
         * compute the mean of remainings, (include leavers who were remainings at that time)
         * and the three bins. find out which bin the leavers belong to. But they are the leavers for t_i.
         */

        // TODO: check out commands: Longitudinal IV - Productivity Leavers & pct of high productive leavers
        // TODO: combine that with: Longitudinal IV - Three Bins for leavers


        // ** Work on user article productivity
        // Have the edits grouped by users, wikiproject, time intervals
        util.runQuery("SELECT t1.user_id AS user_id," +
                        "t2.nwikiproject AS nwikiproject," +
                        "t2.tcount AS tcount," +
                        "t1.timestamp AS timestamp," +
                        "FROM " + util.tableName("bowen_wikis_quitting", "revs_ns0_encoded_valid_users", "t1") +
                        "INNER JOIN " + util.tableName(defaultDataset, "script_user_wp_active_range_revs45"+timeIntervalUnit, "t2") +
                        "ON t1.user_id = t2.user_id " +
                        "WHERE t1.timestamp < t2.tcount_end_ts AND t1.timestamp > t2.tcount_start_ts",
                TableId.of(defaultDataset, "script_ns0_user_wp_edits_records"+timeIntervalUnit));

        util.runQuery("SELECT user_id," +
                        "nwikiproject," +
                        "tcount," +
                        "COUNT(*) AS aggr_productivity," +
                        "FROM " + util.tableName(defaultDataset, "script_ns0_user_wp_edits_records" + timeIntervalUnit) +
                        "GROUP BY user_id, nwikiproject, tcount",
                TableId.of(defaultDataset, "script_ns0_user_wp_tcount_aggr"+timeIntervalUnit));

        // fill the missing tcounts
        util.runQuery("SELECT t1.user_id AS user_id," +
                        "t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "IF (t2.nwikiproject IS NULL AND t2.tcount IS NULL, 0, t2.aggr_productivity) AS aggr_productivity," +
                        "FROM " + util.tableName(defaultDataset, "script_user_wp_active_range_revs45"+timeIntervalUnit, "t1") +
                        "LEFT JOIN " + util.tableName(defaultDataset, "script_ns0_user_wp_tcount_aggr"+timeIntervalUnit, "t2") +
                        "ON t1.user_id = t2.user_id AND t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount ",
                        //"ORDER BY nwikiproject, tcount",
                TableId.of(defaultDataset, "script_ns0_user_wp_tcount_aggr_full"+timeIntervalUnit));

        // adding the time range on each editor for each project he involved
        util.runQuery("SELECT t1.user_id AS user_id," +
                        "t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "(t1.tcount-1) AS pre_tcount," +
                        "t1.aggr_productivity AS aggr_productivity," +
                        "t2.first_tcount AS first_tcount," +
                        "t2.last_tcount AS last_tcount," +
                        "t2.leaving_tcount AS leaving_tcount," +
                        "FROM " + util.tableName(defaultDataset, "script_ns0_user_wp_tcount_aggr_full"+timeIntervalUnit, "t1") +
                        "INNER JOIN " + util.tableName(defaultDataset, "script_user_wp_active_range_revs45"+timeIntervalUnit, "t2") +
                        "ON t1.user_id = t2.user_id AND t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount",
                TableId.of(defaultDataset, "script_ns0_user_wp_tcount_aggr_full_with_range"+timeIntervalUnit));

        // connect to the productivity in the previous time interval
        util.runQuery("SELECT t1.user_id AS user_id," +
                        "t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "t1.aggr_productivity AS cur_aggr_productivity," +
                        "t2.aggr_productivity AS pre_aggr_productivity," +
                        "t1.pre_tcount AS pre_tcount," +
                        "t1.first_tcount AS first_tcount," +
                        "t1.last_tcount AS last_tcount," +
                        "FROM " + util.tableName(defaultDataset, "script_ns0_user_wp_tcount_aggr_full_with_range"+timeIntervalUnit, "t1") +
                        "INNER JOIN " + util.tableName(defaultDataset, "script_ns0_user_wp_tcount_aggr_full_with_range"+timeIntervalUnit, "t2") +
                        "ON t1.user_id = t2.user_id AND t1.nwikiproject = t2.nwikiproject AND t1.pre_tcount = t2.tcount ",
                TableId.of(defaultDataset, "script_ns0_user_wp_tcount_aggr_full_with_range_pre"+timeIntervalUnit));

        // select only remaining members, and compute the mean and stdv for each tcount of projects
        util.runQuery("SELECT nwikiproject," +
                        "tcount," +
                        "AVG(pre_aggr_productivity) AS pre_prod_mean," +
                        "IFNULL(STDDEV(pre_aggr_productivity), 0) AS pre_prod_stdv," +
                        "COUNT(*) AS nbr_remainings," +
                        "FROM " + util.tableName(defaultDataset, "script_ns0_user_wp_tcount_aggr_full_with_range_pre"+timeIntervalUnit) +
                        "WHERE first_tcount != tcount " +
                        "GROUP BY nwikiproject, tcount",
                TableId.of(defaultDataset, "script_ns0_user_wp_tcount_stats"+timeIntervalUnit));

        // compute high and low bars for productivity for each wikiproject in each time interval
        util.runQuery("SELECT nwikiproject," +
                        "tcount," +
                        "pre_prod_mean AS pre_prod_mean," +
                        "(pre_prod_mean + pre_prod_stdv) AS high_bar," +
                        "IF((pre_prod_mean - pre_prod_stdv) < 0, 0, pre_prod_mean - pre_prod_stdv) AS low_bar," +
                        "nbr_remainings AS nbr_remainings," +
                        "FROM " + util.tableName(defaultDataset, "script_ns0_user_wp_tcount_stats"+timeIntervalUnit),
                TableId.of(defaultDataset, "script_ns0_user_wp_tcount_bars"+timeIntervalUnit));

        // high productivity leavers
        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "t2.user_id AS user_id," +
                        "t1.pre_prod_mean AS mean_val," +
                        "t1.high_bar AS high_bar," +
                        "t1.low_bar AS low_bar," +
                        "t1.nbr_remainings AS nbr_remainings," +
                        "t2.pre_aggr_productivity AS pre_aggr_productivity," +
                        "FROM " + util.tableName(defaultDataset, "script_ns0_user_wp_tcount_bars"+timeIntervalUnit, "t1") +
                        "LEFT JOIN " + util.tableName(defaultDataset, "script_ns0_user_wp_tcount_aggr_full_with_range_pre"+timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount " +
                        "WHERE t2.pre_aggr_productivity > t1.high_bar",
                TableId.of(defaultDataset, "script_ns0_stdev_bins_high_prod_leavers_userids"+timeIntervalUnit));

        util.runQuery("SELECT nwikiproject," +
                        "tcount," +
                        "COUNT(*) AS nbr_high_prod_leavers," +
                        "FROM " + util.tableName(defaultDataset, "script_ns0_stdev_bins_high_prod_leavers_userids"+timeIntervalUnit) +
                        "GROUP BY nwikiproject, tcount",
                TableId.of(defaultDataset, "script_ns0_stdev_bins_high_prod_leavers_nbrs"+timeIntervalUnit));

        // fill the missing ones
        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "IF(t2.nwikiproject IS NULL AND t2.tcount IS NULL, 0, t2.nbr_high_prod_leavers) AS nbr_high_prod_leavers," +
                        "FROM " + util.tableName(defaultDataset, "script_user_wp_revs_45_valid_users_wps_valid_range"+timeIntervalUnit, "t1") +
                        "LEFT JOIN " + util.tableName(defaultDataset, "script_ns0_stdev_bins_high_prod_leavers_nbrs" + timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount",
                TableId.of(defaultDataset, "script_ns0_stdev_bins_high_prod_leavers_nbrs_full" + timeIntervalUnit));

        // ** low productivity leavers
        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "t2.user_id AS user_id," +
                        "t1.pre_prod_mean AS mean_val," +
                        "t1.high_bar AS high_bar," +
                        "t1.low_bar AS low_bar," +
                        "t2.pre_aggr_productivity AS pre_aggr_productivity," +
                        "FROM " + util.tableName(defaultDataset, "script_ns0_user_wp_tcount_bars"+timeIntervalUnit, "t1") +
                        "LEFT JOIN " + util.tableName(defaultDataset, "script_ns0_user_wp_tcount_aggr_full_with_range_pre"+timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount " +
                        "WHERE t2.pre_aggr_productivity < t1.low_bar ",
                TableId.of(defaultDataset, "script_ns0_stdev_bins_low_prod_leavers_userids"+timeIntervalUnit));

        util.runQuery("SELECT nwikiproject," +
                        "tcount," +
                        "COUNT(*) AS nbr_low_prod_leavers," +
                        "FROM " + util.tableName(defaultDataset, "script_ns0_stdev_bins_low_prod_leavers_userids"+timeIntervalUnit) +
                        "GROUP BY nwikiproject, tcount",
                TableId.of(defaultDataset, "script_ns0_stdev_bins_low_prod_leavers_nbrs"+timeIntervalUnit));

        // fill the missing ones
        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "IF(t2.nwikiproject IS NULL AND t2.tcount IS NULL, 0, t2.nbr_low_prod_leavers) AS nbr_low_prod_leavers," +
                        "FROM " + util.tableName(defaultDataset, "script_user_wp_revs_45_valid_users_wps_valid_range"+timeIntervalUnit, "t1") +
                        "LEFT JOIN " + util.tableName(defaultDataset, "script_ns0_stdev_bins_low_prod_leavers_nbrs" + timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount",
                TableId.of(defaultDataset, "script_ns0_stdev_bins_low_prod_leavers_nbrs_full" + timeIntervalUnit));

        //** Work on user project coordinations

        // raw edits on ns 4 and 5: lng_user_wikiproject_valid_revs_45

        // Have the edits grouped by users, wikiproject, time intervals
        util.runQuery("SELECT t1.user_id AS user_id," +
                        "t2.nwikiproject AS nwikiproject," +
                        "t2.tcount AS tcount," +
                        "t1.timestamp AS timestamp," +
                        "FROM " + util.tableName("bowen_wikis_quitting", "lng_user_wikiproject_valid_revs_45", "t1") +
                        "INNER JOIN " + util.tableName(defaultDataset, "script_user_wp_active_range_revs45"+timeIntervalUnit, "t2") +
                        "ON t1.user_id = t2.user_id " +
                        "WHERE t1.timestamp < t2.tcount_end_ts AND t1.timestamp > t2.tcount_start_ts",
                TableId.of(defaultDataset, "script_ns45_user_wp_edits_records"+timeIntervalUnit));

        util.runQuery("SELECT user_id," +
                        "nwikiproject," +
                        "tcount," +
                        "COUNT(*) AS aggr_coordination45," +
                        "FROM " + util.tableName(defaultDataset, "script_ns45_user_wp_edits_records" + timeIntervalUnit) +
                        "GROUP BY user_id, nwikiproject, tcount",
                TableId.of(defaultDataset, "script_ns45_user_wp_tcount_aggr"+timeIntervalUnit));

        // fill the missing tcounts
        util.runQuery("SELECT t1.user_id AS user_id," +
                        "t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "IF (t2.nwikiproject IS NULL AND t2.tcount IS NULL, 0, t2.aggr_coordination45) AS aggr_coordination45," +
                        "FROM " + util.tableName(defaultDataset, "script_user_wp_active_range_revs45"+timeIntervalUnit, "t1") +
                        "LEFT JOIN " + util.tableName(defaultDataset, "script_ns45_user_wp_tcount_aggr"+timeIntervalUnit, "t2") +
                        "ON t1.user_id = t2.user_id AND t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount " +
                        "ORDER BY nwikiproject, tcount",
                TableId.of(defaultDataset, "script_ns45_user_wp_tcount_aggr_full"+timeIntervalUnit));

        // adding the time range on each editor for each project he involved
        util.runQuery("SELECT t1.user_id AS user_id," +
                        "t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "(t1.tcount-1) AS pre_tcount," +
                        "t1.aggr_coordination45 AS aggr_coordination45," +
                        "t2.first_tcount AS first_tcount," +
                        "t2.last_tcount AS last_tcount," +
                        "t2.leaving_tcount AS leaving_tcount," +
                        "FROM " + util.tableName(defaultDataset, "script_ns45_user_wp_tcount_aggr_full"+timeIntervalUnit, "t1") +
                        "INNER JOIN " + util.tableName(defaultDataset, "script_user_wp_active_range_revs45"+timeIntervalUnit, "t2") +
                        "ON t1.user_id = t2.user_id AND t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount",
                TableId.of(defaultDataset, "script_ns45_user_wp_tcount_aggr_full_with_range"+timeIntervalUnit));

        // connect to the productivity in the previous time interval
        util.runQuery("SELECT t1.user_id AS user_id," +
                        "t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "t1.aggr_coordination45 AS cur_aggr_coordination45," +
                        "t2.aggr_coordination45 AS pre_aggr_coordination45," +
                        "t1.pre_tcount AS pre_tcount," +
                        "t1.first_tcount AS first_tcount," +
                        "t1.last_tcount AS last_tcount," +
                        "FROM " + util.tableName(defaultDataset, "script_ns45_user_wp_tcount_aggr_full_with_range"+timeIntervalUnit, "t1") +
                        "INNER JOIN " + util.tableName(defaultDataset, "script_ns45_user_wp_tcount_aggr_full_with_range"+timeIntervalUnit, "t2") +
                        "ON t1.user_id = t2.user_id AND t1.nwikiproject = t2.nwikiproject AND t1.pre_tcount = t2.tcount ",
                TableId.of(defaultDataset, "script_ns45_user_wp_tcount_aggr_full_with_range_pre"+timeIntervalUnit));

        util.runQuery("SELECT t1.user_id AS user_id," +
                        "t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "t1.aggr_productivity AS cur_aggr_productivity," +
                        "t2.aggr_productivity AS pre_aggr_productivity," +
                        "t1.pre_tcount AS pre_tcount," +
                        "t1.first_tcount AS first_tcount," +
                        "t1.last_tcount AS last_tcount," +
                        "FROM " + util.tableName(defaultDataset, "script_ns0_user_wp_tcount_aggr_full_with_range"+timeIntervalUnit, "t1") +
                        "INNER JOIN " + util.tableName(defaultDataset, "script_ns0_user_wp_tcount_aggr_full_with_range"+timeIntervalUnit, "t2") +
                        "ON t1.user_id = t2.user_id AND t1.nwikiproject = t2.nwikiproject AND t1.pre_tcount = t2.tcount ",
                TableId.of(defaultDataset, "script_ns0_user_wp_tcount_aggr_full_with_range_pre"+timeIntervalUnit));

        // select only remaining members, and compute the mean and stdv for each tcount of projects
        util.runQuery("SELECT nwikiproject," +
                        "tcount," +
                        "AVG(pre_aggr_coordination45) AS pre_coors_mean," +
                        "IFNULL(STDDEV(pre_aggr_coordination45), 0) AS pre_coors_stdv," +
                        "COUNT(*) AS nbr_remainings," +
                        "FROM " + util.tableName(defaultDataset, "script_ns45_user_wp_tcount_aggr_full_with_range_pre"+timeIntervalUnit) +
                        "WHERE first_tcount != tcount " +
                        "GROUP BY nwikiproject, tcount",
                TableId.of(defaultDataset, "script_ns45_user_wp_tcount_stats"+timeIntervalUnit));

        // compute high and low bars for coordination for each wikiproject in each time interval
        util.runQuery("SELECT nwikiproject," +
                        "tcount," +
                        "pre_coors_mean AS pre_coors_mean," +
                        "(pre_coors_mean + pre_coors_stdv) AS high_bar," +
                        "IF((pre_coors_mean - pre_coors_stdv) < 0, 0, pre_coors_mean - pre_coors_stdv) AS low_bar," +
                        "nbr_remainings AS nbr_remainings," +
                        "FROM " + util.tableName(defaultDataset, "script_ns45_user_wp_tcount_stats"+timeIntervalUnit),
                TableId.of(defaultDataset, "script_ns45_user_wp_tcount_bars"+timeIntervalUnit));

        // high coordination leavers
        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "t2.user_id AS user_id," +
                        "t1.pre_coors_mean AS mean_val," +
                        "t1.high_bar AS high_bar," +
                        "t1.low_bar AS low_bar," +
                        "t1.nbr_remainings AS nbr_remainings," +
                        "t2.pre_aggr_coordination45 AS pre_aggr_coordination45," +
                        "FROM " + util.tableName(defaultDataset, "script_ns45_user_wp_tcount_bars"+timeIntervalUnit, "t1") +
                        "LEFT JOIN " + util.tableName(defaultDataset, "script_ns45_user_wp_tcount_aggr_full_with_range_pre"+timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount " +
                        "WHERE t2.pre_aggr_coordination45 > t1.high_bar",
                TableId.of(defaultDataset, "script_ns45_stdev_bins_high_coors_leavers_userids"+timeIntervalUnit));

        util.runQuery("SELECT nwikiproject," +
                        "tcount," +
                        "COUNT(*) AS nbr_high_coors_leavers," +
                        "FROM " + util.tableName(defaultDataset, "script_ns45_stdev_bins_high_coors_leavers_userids"+timeIntervalUnit) +
                        "GROUP BY nwikiproject, tcount",
                TableId.of(defaultDataset, "script_ns45_stdev_bins_high_coors_leavers_nbrs"+timeIntervalUnit));

        // fill the missing ones
        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "IF(t2.nwikiproject IS NULL AND t2.tcount IS NULL, 0, t2.nbr_high_coors_leavers) AS nbr_high_coors_leavers," +
                        "FROM " + util.tableName(defaultDataset, "script_user_wp_revs_45_valid_users_wps_valid_range"+timeIntervalUnit, "t1") +
                        "LEFT JOIN " + util.tableName(defaultDataset, "script_ns45_stdev_bins_high_coors_leavers_nbrs" + timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount",
                TableId.of(defaultDataset, "script_ns45_stdev_bins_high_coors_leavers_nbrs_full" + timeIntervalUnit));

        // ** low coordination leavers
        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "t2.user_id AS user_id," +
                        "t1.pre_coors_mean AS mean_val," +
                        "t1.high_bar AS high_bar," +
                        "t1.low_bar AS low_bar," +
                        "t2.pre_aggr_coordination45 AS pre_aggr_coordination45," +
                        "FROM " + util.tableName(defaultDataset, "script_ns45_user_wp_tcount_bars"+timeIntervalUnit, "t1") +
                        "LEFT JOIN " + util.tableName(defaultDataset, "script_ns45_user_wp_tcount_aggr_full_with_range_pre"+timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount " +
                        "WHERE t2.pre_aggr_coordination45 < t1.low_bar ",
                TableId.of(defaultDataset, "script_ns45_stdev_bins_low_coors_leavers_userids"+timeIntervalUnit));

        util.runQuery("SELECT nwikiproject," +
                        "tcount," +
                        "COUNT(*) AS nbr_low_coors_leavers," +
                        "FROM " + util.tableName(defaultDataset, "script_ns45_stdev_bins_low_coors_leavers_userids"+timeIntervalUnit) +
                        "GROUP BY nwikiproject, tcount",
                TableId.of(defaultDataset, "script_ns45_stdev_bins_low_coors_leavers_nbrs"+timeIntervalUnit));

        // fill the missing ones
        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "IF(t2.nwikiproject IS NULL AND t2.tcount IS NULL, 0, t2.nbr_low_coors_leavers) AS nbr_low_coors_leavers," +
                        "FROM " + util.tableName(defaultDataset, "script_user_wp_revs_45_valid_users_wps_valid_range"+timeIntervalUnit, "t1") +
                        "LEFT JOIN " + util.tableName(defaultDataset, "script_ns45_stdev_bins_low_coors_leavers_nbrs" + timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount",
                TableId.of(defaultDataset, "script_ns45_stdev_bins_low_coors_leavers_nbrs_full" + timeIntervalUnit));

        // Newcomer's prior experience - number of WPs joined
        // Longitudinal IV - Newcomer Prior Experience by # of WPs Joined
    }

    private void computeControlVariables() throws Exception {

        util.runQuery("SELECT nwikiproject," +
                        "MIN(first_edit) AS wp_creation_ts," +
                        "FROM " + util.tableName("bowen_wikis_quitting", "rev_ns45_user_wp_global_tcount_range_valid") +
                        "GROUP BY nwikiproject",
                TableId.of(defaultDataset, "script_wp_creation_ts"));

        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t2.tcount AS tcount," +
                        "IF(t2.start_ts - t1.wp_creation_ts > 0, t2.start_ts - t1.wp_creation_ts, 0) AS wp_tenure," +
                        "FROM " + util.tableName(defaultDataset, "script_wp_creation_ts", "t1") +
                        "JOIN " + util.tableName(defaultDataset, "script_user_wp_revs_45_valid_users_wps_valid_range"+timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject",
                TableId.of(defaultDataset, "script_cv_project_tenure_partial"+timeIntervalUnit));

        util.runQuery("SELECT nwikiproject," +
                        "tcount," +
                        "wp_tenure / (3600*24*30*"+ timeIntervalUnit +") AS wp_tenure," +
                        "FROM " + util.tableName(defaultDataset, "script_cv_project_tenure_partial"+timeIntervalUnit) +
//                        "WHERE wp_tenure != 0" +
                        "ORDER BY nwikiproject, tcount",
                TableId.of(defaultDataset, "script_cv_project_tenure"+timeIntervalUnit));

    }

    private void mergeCVAndNewEditorMemberships() throws Exception {
        computeControlVariables();

        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "t1.newcomers_nbr AS newcomers_nbr," +
                        "t1.leavers_nbr AS leavers_nbr," +
                        "t1.remainings_nbr AS remainings_nbr," +
                        "t1.remainings_prod0 AS remainings_prod0," +
                        "t1.remainings_coors45 AS remainings_coors45," +
                        "t1.remainings_art_comm1 AS remainings_art_comm1," +
                        "t1.remainings_user_comm3 AS remainings_user_comm3," +
                        "t1.superficial_leavers_cnt AS superficial_leavers_cnt," +
                        "t1.substantive_leavers_cnt AS substantive_leavers_cnt," +
                        "t2.wp_tenure AS wp_tenure," +
                        "FROM " + util.tableName(defaultDataset, "script_wp_newcomers_sup_sub_leavers_remainings_nbr_prod_coors_ns13_tcount_full"+timeIntervalUnit+"edit"+monthlyEditingThreshold, "t1") +
                        "INNER JOIN " + util.tableName(defaultDataset, "script_cv_project_tenure"+timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount",
                TableId.of(defaultDataset, "script_results_wp_ivs_cv"+timeIntervalUnit+"edit"+monthlyEditingThreshold));
    }


    private void mergeDVsAndMemberComposition() throws Exception {

        // merge newcomers and leavers nbr
        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "t1.newcomers_nbr AS newcomers_nbr," +
                        "t2.remainings_nbr AS remainings_nbr," +
                        "FROM " + util.tableName(defaultDataset, "script_wp_newcomers_nbr_tcount_full" + timeIntervalUnit, "t1") +
                        "INNER JOIN " + util.tableName(defaultDataset, "script_wp_remainings_nbr_tcount_full" + timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount",
                TableId.of(defaultDataset, "script_merging_newcomers_remainings"+timeIntervalUnit));

        // merge with leavers nbr
        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "t1.newcomers_nbr AS newcomers_nbr," +
                        "t1.remainings_nbr AS remainings_nbr," +
                        "t2.leavers_nbr AS leavers_nbr," +
                        "FROM " + util.tableName(defaultDataset, "script_merging_newcomers_remainings" + timeIntervalUnit, "t1") +
                        "INNER JOIN " + util.tableName(defaultDataset, "script_wp_leavers_nbr_tcount_full" + timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount",
                TableId.of(defaultDataset, "script_merging_newcomers_remainings_leavers"+timeIntervalUnit));

        // merge with dv prod
        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "t1.newcomers_nbr AS newcomers_nbr," +
                        "t1.remainings_nbr AS remainings_nbr," +
                        "t1.leavers_nbr AS leavers_nbr," +
                        "t2.group_article_productivity AS group_article_productivity," +
                        "FROM " + util.tableName(defaultDataset, "script_merging_newcomers_remainings_leavers" + timeIntervalUnit, "t1") +
                        "INNER JOIN " + util.tableName(defaultDataset, "dv_wp_full_mbr_prod0_per_time_interval_months" + timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount",
                TableId.of(defaultDataset, "script_mbr_comp_dv_prod"+timeIntervalUnit));

        // merge with dv coor
        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "t1.newcomers_nbr AS newcomers_nbr," +
                        "t1.remainings_nbr AS remainings_nbr," +
                        "t1.leavers_nbr AS leavers_nbr," +
                        "t1.group_article_productivity AS group_article_productivity," +
                        "t2.project_coors AS project_coors," +
                        "FROM " + util.tableName(defaultDataset, "script_mbr_comp_dv_prod"+timeIntervalUnit, "t1") +
                        "INNER JOIN " + util.tableName(defaultDataset, "dv_wp_full_coor45_per_time_interval_months" + timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount",
                TableId.of(defaultDataset, "script_mbr_comp_dv_prod_coors"+timeIntervalUnit));

        // merge with dv art_comm
        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "t1.newcomers_nbr AS newcomers_nbr," +
                        "t1.remainings_nbr AS remainings_nbr," +
                        "t1.leavers_nbr AS leavers_nbr," +
                        "t1.group_article_productivity AS group_article_productivity," +
                        "t1.project_coors AS project_coors," +
                        "t2.project_art_comm AS project_art_comm," +
                        "FROM " + util.tableName(defaultDataset, "script_mbr_comp_dv_prod_coors"+timeIntervalUnit, "t1") +
                        "INNER JOIN " + util.tableName(defaultDataset, "dv_wp_full_art_comm1_per_time_interval_months" + timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount",
                TableId.of(defaultDataset, "script_mbr_comp_dv_prod_coors_art_comm"+timeIntervalUnit));

        // merge with dv user comm
        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "t1.newcomers_nbr AS newcomers_nbr," +
                        "t1.remainings_nbr AS remainings_nbr," +
                        "t1.leavers_nbr AS leavers_nbr," +
                        "t1.group_article_productivity AS group_article_productivity," +
                        "t1.project_coors AS project_coors," +
                        "t1.project_art_comm AS project_art_comm," +
                        "IFNULL(t2.project_user_comm, 0) AS project_user_comm," + // user comm is missing some projects
                        "FROM " + util.tableName(defaultDataset, "script_mbr_comp_dv_prod_coors_art_comm"+timeIntervalUnit, "t1") +
                        "LEFT JOIN " + util.tableName(defaultDataset, "dv_wp_full_user_comm3_per_time_interval_months" + timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount",
                TableId.of(defaultDataset, "script_mbr_comp_dv_prod_coors_art_comm_user_comm"+timeIntervalUnit));
    }

    /**
     * Identify the numbers of newcomers, leavers, and remaining members of each project in each time interval.
     * - create editor-wp-tcount table
     *
     */
    private void identifyMembersInProjects() throws Exception {
        // starting from table: (Find the first and last time interval for each editor on wikiprojects (6-month interval))
        // in file Queries and Tables

        // Locate the first edit to a time interval of an editor to a project
        util.runQuery("SELECT t1.user_id AS user_id," +
                        "t1.nwikiproject AS nwikiproject," +
                        "t1.first_edit AS first_edit," +
                        "t2.tcount AS first_tcount," +
                        "t1.last_edit AS last_edit," +
                        "FROM " + util.tableName("bowen_wikis_quitting", "rev_ns45_user_wikiproject_ts_range_encoded", "t1") +
                        "INNER JOIN " + util.tableName(defaultDataset, "script_user_wp_revs_45_valid_users_wps_valid_range" + timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject " +
                        "WHERE t2.start_ts <= t1.first_edit AND t1.first_edit < t2.end_ts ",
                TableId.of(defaultDataset, "script_user_wp_active_start_end_revs45_partial"+timeIntervalUnit));

        // Locate the last edit for the editor on each project involved
        util.runQuery("SELECT t1.user_id AS user_id," +
                        "t1.nwikiproject AS nwikiproject," +
                        "t1.first_edit AS first_edit," +
                        "t1.first_tcount AS first_tcount," +
                        "t1.last_edit AS last_edit," +
                        "t2.tcount AS last_tcount," +
                        "FROM " + util.tableName(defaultDataset, "script_user_wp_active_start_end_revs45_partial" + timeIntervalUnit, "t1") +
                        "INNER JOIN " + util.tableName(defaultDataset, "script_user_wp_revs_45_valid_users_wps_valid_range" + timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject " +
                        "WHERE t2.start_ts <= t1.last_edit AND t1.last_edit < t2.end_ts",
                TableId.of(defaultDataset, "script_user_wp_active_start_end_revs45"+timeIntervalUnit));

        // Create the entire active range of the editor on the project by filling the gaps
        util.runQuery("SELECT t1.user_id AS user_id," +
                        "t1.nwikiproject AS nwikiproject," +
                        "t2.start_ts AS tcount_start_ts," +
                        "t2.end_ts AS tcount_end_ts," +
                        "t2.tcount AS tcount," +
                        "t1.first_tcount AS first_tcount," +
                        "t1.last_tcount AS last_tcount," +
                        "(t1.last_tcount+1) AS leaving_tcount," +
                        "FROM " + util.tableName(defaultDataset, "script_user_wp_active_start_end_revs45" + timeIntervalUnit, "t1") +
                        "INNER JOIN " + util.tableName(defaultDataset, "script_user_wp_revs_45_valid_users_wps_valid_range" + timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject " +
                        "WHERE t1.first_edit < t2.end_ts AND t1.last_edit > t2.start_ts " +
                        "ORDER BY nwikiproject, user_id, tcount",
                TableId.of(defaultDataset, "script_user_wp_active_range_revs45"+timeIntervalUnit));

        // Compute newcomers for each project in each time interval
        util.runQuery("SELECT nwikiproject," +
                        "tcount," +
                        "COUNT(UNIQUE(user_id)) AS newcomers_nbr," +
                        "FROM " + util.tableName(defaultDataset, "script_user_wp_active_range_revs45" + timeIntervalUnit) +
                        "WHERE tcount = first_tcount " +
                        "GROUP BY nwikiproject, tcount",
                TableId.of(defaultDataset, "script_wp_newcomers_nbr_tcount" + timeIntervalUnit));
        // add in time intervals with no such members
        // TODO: do not count the first and last time intervals - to remove (identify the value)
        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "IFNULL(t2.newcomers_nbr, 0) AS newcomers_nbr," +
                        "FROM " + util.tableName(defaultDataset, "script_user_wp_revs_45_valid_users_wps_valid_range" + timeIntervalUnit, "t1") +
                        "LEFT JOIN " + util.tableName(defaultDataset, "script_wp_newcomers_nbr_tcount" + timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount " +
                        "WHERE t1.tcount != 0", // TODO: check the value
                TableId.of(defaultDataset, "script_wp_newcomers_nbr_tcount_full"+timeIntervalUnit));

        // leavers
        util.runQuery("SELECT nwikiproject," +
                        "tcount+1 AS tcount," +
                        "COUNT(UNIQUE(user_id)) AS leavers_nbr," +
                        "FROM " + util.tableName(defaultDataset, "script_user_wp_active_range_revs45" + timeIntervalUnit) +
                        "WHERE (tcount+1) = leaving_tcount " +
                        "GROUP BY nwikiproject, tcount",
                TableId.of(defaultDataset, "script_wp_leavers_nbr_tcount" + timeIntervalUnit));

        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "IFNULL(t2.leavers_nbr, 0) AS leavers_nbr," +
                        "FROM " + util.tableName(defaultDataset, "script_user_wp_revs_45_valid_users_wps_valid_range" + timeIntervalUnit, "t1") +
                        "LEFT JOIN " + util.tableName(defaultDataset, "script_wp_leavers_nbr_tcount" + timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount " +
                        "WHERE t1.tcount != 0", // TODO check the last time interval value
                TableId.of(defaultDataset, "script_wp_leavers_nbr_tcount_full"+timeIntervalUnit));

        // remaining members
        util.runQuery("SELECT nwikiproject," +
                        "tcount," +
                        "COUNT(UNIQUE(user_id)) AS remainings_nbr," +
                        "FROM " + util.tableName(defaultDataset, "script_user_wp_active_range_revs45" + timeIntervalUnit) +
                        "WHERE tcount != first_tcount " +
                        "GROUP BY nwikiproject, tcount",
                TableId.of(defaultDataset, "script_wp_remainings_nbr_tcount"+timeIntervalUnit));

        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "IFNULL(t2.remainings_nbr, 0) AS remainings_nbr," +
                        "FROM " + util.tableName(defaultDataset, "script_user_wp_revs_45_valid_users_wps_valid_range" + timeIntervalUnit, "t1") +
                        "LEFT JOIN " + util.tableName(defaultDataset, "script_wp_remainings_nbr_tcount" + timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount " +
                        "WHERE t1.tcount != 0", // TODO check the last time interval value
                TableId.of(defaultDataset, "script_wp_remainings_nbr_tcount_full"+timeIntervalUnit));
    }


    /**
     * Compute DVs in the each time interval for each wikiproject
     * DVs are only from valid members and valid wikiprojects
     * TODO: why user comm is high? Editors are still talking even they are no longer involved in the project.
     */
    private void createLongitudinalProjectLevelDVs() throws Exception {

        // DV - coordination: total number of coordination in ns 45 of each project in each time interval
        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t2.tcount AS tcount," +
                        "COUNT(*) AS project_coordination," +
                        "FROM " + util.tableName("bowen_wikis_quitting", "rev_ns45_user_wikiproject_ts_encoded_full", "t1") +
                        "INNER JOIN " + util.tableName(defaultDataset, "script_user_wp_revs_45_valid_users_wps_valid_range"+timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject " +
                        "WHERE t1.timestamp >= t2.start_ts AND t1.timestamp < t2.end_ts " +
                        "GROUP BY nwikiproject, tcount",
                TableId.of(defaultDataset, "dv_wp_coor45_per_time_interval_months"+timeIntervalUnit));

        // fill the values for the missing time intervals todo: only time project level needed tcount, or users?
        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "IF (t2.nwikiproject IS NULL AND t2.tcount IS NULL, 0, t2.project_coordination) AS project_coors," +
                        "FROM " + util.tableName(defaultDataset, "script_user_wp_revs_45_valid_users_wps_valid_range"+timeIntervalUnit, "t1") +
                        "LEFT JOIN " + util.tableName(defaultDataset, "dv_wp_coor45_per_time_interval_months"+timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount " +
                        "ORDER BY nwikiproject, tcount",
                TableId.of(defaultDataset, "dv_wp_full_coor45_per_time_interval_months"+timeIntervalUnit));

        // DV: coordination: ns 4
        util.runQuery("SELECT t1.user_text AS user_text," +
                        "t2.user_id AS user_id," +
                        "t1.wikiproject AS wikiproject," +
                        "t1.timestamp AS timestamp," +
                        "t1.ns AS ns," +
                        "FROM " + util.tableName("bowen_wikis_quitting", "rev_ns4_user_wikiproject_ts", "t1") +
                        "INNER JOIN " + util.tableName("bowen_editor_attachments", "users_valid_withIds", "t2") +
                        "ON t1.user_text = t2.user_text",
                TableId.of(defaultDataset, "script_rev_ns4_user_wikiproject_ts_encoded_partial"));

        util.runQuery("SELECT t1.user_text AS user_text," +
                        "t1.user_id AS user_id," +
                        "t1.wikiproject AS wikiproject," +
                        "t2.nwikiproject AS nwikiproject," +
                        "t1.timestamp AS timestamp," +
                        "t1.ns AS ns," +
                        "FROM " + util.tableName(defaultDataset, "script_rev_ns4_user_wikiproject_ts_encoded_partial", "t1") +
                        "INNER JOIN " + util.tableName("bowen_editor_attachments", "wikiprojects_valid_withIds_3members", "t2") +
                        "ON t1.wikiproject = t2.wikiproject",
                TableId.of(defaultDataset, "script_rev_ns4_user_wikiproject_ts_encoded_full"));

        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t2.tcount AS tcount," +
                        "COUNT(*) AS project_coordination4," +
                        "FROM " + util.tableName(defaultDataset, "script_rev_ns4_user_wikiproject_ts_encoded_full", "t1") +
                        "INNER JOIN " + util.tableName(defaultDataset, "script_user_wp_revs_45_valid_users_wps_valid_range"+timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject " +
                        "WHERE t1.timestamp >= t2.start_ts AND t1.timestamp < t2.end_ts " +
                        "GROUP BY nwikiproject, tcount",
                TableId.of(defaultDataset, "dv_wp_coor4_per_time_interval_months"+timeIntervalUnit));

        // fill the values for the missing time intervals todo: only time project level needed tcount, or users?
        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "IF (t2.nwikiproject IS NULL AND t2.tcount IS NULL, 0, t2.project_coordination4) AS project_coor4," +
                        "FROM " + util.tableName(defaultDataset, "script_user_wp_revs_45_valid_users_wps_valid_range"+timeIntervalUnit, "t1") +
                        "LEFT JOIN " + util.tableName(defaultDataset, "dv_wp_coor4_per_time_interval_months"+timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount " +
                        "ORDER BY nwikiproject, tcount",
                TableId.of(defaultDataset, "dv_wp_full_coor4_per_time_interval_months"+timeIntervalUnit));


        // DV: coordination: ns 5
        util.runQuery("SELECT t1.user_text AS user_text," +
                        "t2.user_id AS user_id," +
                        "t1.wikiproject AS wikiproject," +
                        "t1.timestamp AS timestamp," +
                        "t1.ns AS ns," +
                        "FROM " + util.tableName("bowen_wikis_quitting", "rev_ns5_user_wikiproject_ts", "t1") +
                        "INNER JOIN " + util.tableName("bowen_editor_attachments", "users_valid_withIds", "t2") +
                        "ON t1.user_text = t2.user_text",
                TableId.of(defaultDataset, "script_rev_ns5_user_wikiproject_ts_encoded_partial"));

        util.runQuery("SELECT t1.user_text AS user_text," +
                        "t1.user_id AS user_id," +
                        "t1.wikiproject AS wikiproject," +
                        "t2.nwikiproject AS nwikiproject," +
                        "t1.timestamp AS timestamp," +
                        "t1.ns AS ns," +
                        "FROM " + util.tableName(defaultDataset, "script_rev_ns5_user_wikiproject_ts_encoded_partial", "t1") +
                        "INNER JOIN " + util.tableName("bowen_editor_attachments", "wikiprojects_valid_withIds_3members", "t2") +
                        "ON t1.wikiproject = t2.wikiproject",
                TableId.of(defaultDataset, "script_rev_ns5_user_wikiproject_ts_encoded_full"));

        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t2.tcount AS tcount," +
                        "COUNT(*) AS project_coordination5," +
                        "FROM " + util.tableName(defaultDataset, "script_rev_ns5_user_wikiproject_ts_encoded_full", "t1") +
                        "INNER JOIN " + util.tableName(defaultDataset, "script_user_wp_revs_45_valid_users_wps_valid_range"+timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject " +
                        "WHERE t1.timestamp >= t2.start_ts AND t1.timestamp < t2.end_ts " +
                        "GROUP BY nwikiproject, tcount",
                TableId.of(defaultDataset, "dv_wp_coor5_per_time_interval_months"+timeIntervalUnit));

        // fill the values for the missing time intervals todo: only time project level needed tcount, or users?
        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "IF (t2.nwikiproject IS NULL AND t2.tcount IS NULL, 0, t2.project_coordination5) AS project_coor5," +
                        "FROM " + util.tableName(defaultDataset, "script_user_wp_revs_45_valid_users_wps_valid_range"+timeIntervalUnit, "t1") +
                        "LEFT JOIN " + util.tableName(defaultDataset, "dv_wp_coor5_per_time_interval_months"+timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount " +
                        "ORDER BY nwikiproject, tcount",
                TableId.of(defaultDataset, "dv_wp_full_coor5_per_time_interval_months"+timeIntervalUnit));

        // DV -  productivity: total article edits of the members
        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t2.tcount AS tcount," +
                        "COUNT(*) AS group_article_productivity," +
                        "FROM " + util.tableName("bowen_wikis_quitting", "rev_ns0_member_project_edits", "t1") +
                        "INNER JOIN " + util.tableName(defaultDataset, "script_user_wp_revs_45_valid_users_wps_valid_range" + timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject " +
                        "WHERE t1.timestamp >= t2.start_ts AND t1.timestamp < t2.end_ts " +
                        "GROUP BY nwikiproject, tcount " +
                        "ORDER BY nwikiproject, tcount",
                TableId.of(defaultDataset, "dv_wp_mbr_prod0_per_time_interval_months" + timeIntervalUnit));

        // fill the values for the missing time intervals
        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "IF (t2.nwikiproject IS NULL AND t2.tcount IS NULL, 0, t2.group_article_productivity) AS group_article_productivity," +
                        "FROM " + util.tableName(defaultDataset, "script_user_wp_revs_45_valid_users_wps_valid_range"+timeIntervalUnit, "t1") +
                        "LEFT JOIN " + util.tableName(defaultDataset, "dv_wp_mbr_prod0_per_time_interval_months" + timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount " +
                        "ORDER BY nwikiproject, tcount",
                TableId.of(defaultDataset, "dv_wp_full_mbr_prod0_per_time_interval_months" + timeIntervalUnit));

        // DV: article communication
        // TODO: need to check the alignment of time intervals
        // TODO: check rev_ns1_member_project_edits table - article edits??? (confusing name)
        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t2.tcount AS tcount," +
                        "COUNT(*) AS art_comm," +
                        "FROM " + util.tableName("bowen_wikis_quitting", "rev_ns1_member_project_edits", "t1") +
                        "INNER JOIN " + util.tableName(defaultDataset, "script_user_wp_revs_45_valid_users_wps_valid_range" + timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject " +
                        "WHERE t1.timestamp >= t2.start_ts AND t1.timestamp < t2.end_ts " +
                        "GROUP BY nwikiproject, tcount " +
                        "ORDER BY nwikiproject, tcount",
                TableId.of(defaultDataset, "dv_wp_art_comm1_per_time_interval_months"+timeIntervalUnit));

        // fill the values for the missing time intervals
        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "IF (t2.nwikiproject IS NULL AND t2.tcount IS NULL, 0, t2.art_comm) AS project_art_comm," +
                        "FROM " + util.tableName(defaultDataset, "script_user_wp_revs_45_valid_users_wps_valid_range" + timeIntervalUnit, "t1") +
                        "LEFT JOIN " + util.tableName(defaultDataset, "dv_wp_art_comm1_per_time_interval_months" + timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount " +
                        "ORDER BY nwikiproject, tcount",
                TableId.of(defaultDataset, "dv_wp_full_art_comm1_per_time_interval_months"+timeIntervalUnit));

        // DV: user communication (using previous generated table)
        // append nwikiproject to wikiproject
        util.runQuery("SELECT t2.nwikiproject AS nwikiproject," +
                        "t2.wikiproject AS wikiproject," +
                        "t1.user_talk_from AS user_talk_from," +
                        "t1.former_member AS former_member," +
                        "t1.timestamp AS timestamp," +
                        "FROM " + util.tableName("bowen_user_dropouts", "user_talks_to_former_wikiprojects_members", "t1") +
                        "INNER JOIN " + util.tableName("bowen_editor_attachments", "wikiprojects_valid_withIds_3members", "t2") +
                        "ON t1.wikiproject = t2.wikiproject",
                TableId.of(defaultDataset, "script_valid_user_comm_records_encoded"));

        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t2.tcount AS tcount," +
                        "COUNT(*) AS user_comm," +
                        "FROM " + util.tableName(defaultDataset, "script_valid_user_comm_records_encoded", "t1") +
                        "INNER JOIN " + util.tableName(defaultDataset, "script_user_wp_revs_45_valid_users_wps_valid_range" + timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject " +
                        "WHERE t1.timestamp >= t2.start_ts AND t1.timestamp < t2.end_ts " +
                        "GROUP BY nwikiproject, tcount " +
                        "ORDER BY nwikiproject, tcount",
                TableId.of(defaultDataset, "dv_wp_user_comm3_per_time_interval_months" + timeIntervalUnit));

        // fill the values for the missing time intervals
        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t1.tcount AS tcount," +
                        "IF (t2.nwikiproject IS NULL AND t2.tcount IS NULL, 0, t2.user_comm) AS project_user_comm," +
                        "FROM " + util.tableName(defaultDataset, "script_user_wp_revs_45_valid_users_wps_valid_range" + timeIntervalUnit, "t1") +
                        "LEFT JOIN " + util.tableName(defaultDataset, "dv_wp_user_comm3_per_time_interval_months" + timeIntervalUnit, "t2") +
                        "ON t1.nwikiproject = t2.nwikiproject AND t1.tcount = t2.tcount " +
                        "ORDER BY nwikiproject, tcount",
                TableId.of(defaultDataset, "dv_wp_full_user_comm3_per_time_interval_months" + timeIntervalUnit));
    }

    private void createTimeRangeOfAllProjects() throws Exception {

        // merge ns 4 and 5
        util.runQuery("SELECT *" +
                        "FROM bowen_wikis_quitting.rev_ns4_user_wikiproject_ts, bowen_wikis_quitting.rev_ns5_user_wikiproject_ts",
                TableId.of(defaultDataset, "script_rev_ns45_user_wp_ts"));


        // select valid editors
        util.runQuery("SELECT t1.user_text AS user_text," +
                        "t2.user_id AS user_id," +
                        "t1.wikiproject AS wikiproject," +
                        "t1.timestamp AS timestamp," +
                        "t1.ns AS ns," +
                        "FROM " + defaultDataset + "." + "script_rev_ns45_user_wp_ts AS t1 " +
                        "INNER JOIN bowen_editor_attachments.users_valid_withIds AS t2 " +
                        "ON t1.user_text = t2.user_text",
                TableId.of(defaultDataset, "script_user_wp_revs_45_valid_users"));

        // select valid project
        util.runQuery("SELECT t1.user_text AS user_text," +
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
        util.runQuery("SELECT MAX(timestamp) AS maxTS," +
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
        util.runQuery("SELECT nwikiproject," +
                "MAX(timestamp) AS maxTS," +
                "MIN(timestamp) AS minTS," +
                "FROM " + util.tableName(defaultDataset, "script_user_wp_revs_45_valid_users_wps") +
                "GROUP BY nwikiproject", TableId.of(defaultDataset, "script_wp_revs_45_time_range"));

        // create valid project time range
        util.runQuery("SELECT t1.nwikiproject AS nwikiproject," +
                        "t2.tcount AS tcount," +
                        "t2.start_ts AS start_ts," +
                        "t2.end_ts AS end_ts," +
                        "FROM " + util.tableName(defaultDataset, "script_wp_revs_45_time_range", "t1") +
                        "INNER JOIN " + util.tableName(defaultDataset, "full_time_intervals_" + timeIntervalUnit + "month", "t2") +
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
}
