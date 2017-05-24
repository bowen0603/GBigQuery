package com.google.cloud;

import com.google.cloud.bigquery.TableId;
import javafx.scene.control.Tab;
import jdk.internal.org.objectweb.asm.tree.TableSwitchInsnNode;

import java.io.IOException;

/**
 * Created by bobo on 5/4/17.
 */
public class WikiProjectRecsys {

    /**
     * Key Tables:
     * (1) Time to join the first WikiProject: editor_projects_joined_ts_2edits
     */

    // TODO: stats about % of people using talk pages in their first X edits...

    private int recordThreshold;
    private Util util;
    private String defaultDataset;
    private String superficialLeaverAnalysisFile;
    private int timeIntervalUnit; // in months
    private int monthlyEditingThreshold;
    // number of edits on articles within the scope of the project


    WikiProjectRecsys() {
        recordThreshold = 500;
        timeIntervalUnit = 3;
        monthlyEditingThreshold = 5;
//        superficialLeaverAnalysisFile = "superficial_leaver_analysis_ns45_above_0.1_0.2.csv";
        superficialLeaverAnalysisFile = "substantive_leaver_analysis_ns45_0.15_0.2.csv";
        defaultDataset = "WikiProjectRecsys";
        this.util = new Util();
    }

    public static void main(String... args) throws Exception {
        WikiProjectRecsys self = new WikiProjectRecsys();
//        self.projectParticipationStats();

//        self.AnalysizeBestTimingToJoinProjects();

//        self.createCategoryVectorBeforeJoiningFirstWikiProject();

        self.createProjectsEditorsJoined();
    }



    // generate stats about the number of projects editors joined
    private void projectParticipationStats() throws Exception {

        // # of projects joined with different joining requirements
//        util.runQuery("SELECT user_text," +
//                        "COUNT(*) AS projects_joined," +
//                        "FROM " + util.tableName("bowen_editor_attachments", "dv_user_wikiproject_coordination_edits") +
//                        "GROUP BY user_text",
//                TableId.of(defaultDataset, "editor_projects_joined_1edit"));

//        util.runQuery("SELECT user_text," +
//                        "COUNT(*) AS projects_joined," +
//                        "FROM " + util.tableName("bowen_editor_attachments", "dv_user_wikiproject_coordination_edits") +
//                        "WHERE total_edits > 1" +
//                        "GROUP BY user_text ",
//                TableId.of(defaultDataset, "editor_projects_joined_2edits"));

//        util.runQuery("SELECT COUNT(UNIQUE(user_text)) AS user," +
//                "FROM " + util.tableName("bowen_editor_attachments", "dv_user_wikiproject_coordination_edits") +
//                "WHERE total_edits > 1");


        // # of edits made in total
//        util.runQuery("SELECT t1.rev_user_text AS user_text," +
//                        "t1.rev_user_id AS user_id," +
//                        "t1.rev_timestamp AS timestamp," +
//                        "t1.ns AS ns," +
//                        "t1.rev_page_title AS page_title," +
//                        "t1.rev_comment AS comment," +
//                        "FROM " + util.tableName("bowen_user_dropouts", "revs", "t1") +
//                        "INNER JOIN " + util.tableName(defaultDataset, "editor_projects_joined_2edits", "t2") +
//                        "ON t1.rev_user_text = t2.user_text",
//                TableId.of(defaultDataset, "filtered_users_by_projects_2edits"));
//
//
//        util.runQuery("SELECT user_text," +
//                        "COUNT(*) AS total_edits," +
//                        "FROM " + util.tableName(defaultDataset, "filtered_users_by_projects_2edits") +
//                        "GROUP BY user_text",
//                TableId.of(defaultDataset, "filtered_users_by_projects_2edits_total_edits"));
//
//        // tenure in wikipedia
//        util.runQuery("SELECT user_text," +
//                        "MIN(timestamp) AS first_edit," +
//                        "MAX(timestamp) AS last_edit," +
//                        "FROM " + util.tableName(defaultDataset, "filtered_users_by_projects_2edits") +
//                        "GROUP BY user_text",
//                TableId.of(defaultDataset, "filtered_users_by_projects_2edits_firstlast"));
//
//        util.runQuery("SELECT user_text," +
//                        "(last_edit-first_edit) / (3600*24*30) AS tenure_months," +
//                        "FROM " + util.tableName(defaultDataset, "filtered_users_by_projects_2edits_firstlast"),
//                TableId.of(defaultDataset, "filtered_users_by_projects_2edits_tenure"));


        // join those three parts together
//        util.runQuery("SELECT t1.user_text AS user_text," +
//                        "t1.projects_joined AS projects_joined," +
//                        "t2.total_edits AS total_edits," +
//                        "FROM " + util.tableName(defaultDataset, "editor_projects_joined_2edits", "t1") +
//                        "INNER JOIN " + util.tableName(defaultDataset, "filtered_users_by_projects_2edits_total_edits", "t2") +
//                        "ON t1.user_text = t2.user_text",
//                TableId.of(defaultDataset, "filtered_users_by_projects_2edits_joining1"));

        util.runQuery("SELECT t1.user_text AS user_text," +
                        "t1.projects_joined AS projects_joined," +
                        "t1.total_edits AS total_edits," +
                        "t2.tenure_months AS tenure_months," +
                        "FROM " + util.tableName(defaultDataset, "filtered_users_by_projects_2edits_joining1", "t1") +
                        "INNER JOIN " + util.tableName(defaultDataset, "filtered_users_by_projects_2edits_tenure", "t2") +
                        "ON t1.user_text = t2.user_text",
                TableId.of(defaultDataset, "filtered_users_by_projects_2edits_joining2"));
    }

    private void AnalysizeBestTimingToJoinProjects() throws Exception {
        // Distribution of the number of edits editors made on namespace 4 and 5
        // TableId.of(defaultDataset, "editor_projects_joined_1edit"))

        // Distribution of the time (in months) to join the first WikiProjects since joining Wikipedia

        // Note: only consider having the second edits as joining the project
//        util.runQuery("SELECT user_text," +
//                        "COUNT(*) AS projects_joined," +
//                        "MIN(first_edit) AS joining_project_ts," +
//                        "FROM " + util.tableName("bowen_editor_attachments", "dv_user_wikiproject_coordination_edits") +
//                        "WHERE total_edits > 1" +
//                        "GROUP BY user_text ",
//                TableId.of(defaultDataset, "editor_projects_joined_ts_2edits"));
//
//        util.runQuery("SELECT rev_user_text AS user_text," +
//                        "rev_user_id AS user_id," +
//                        "MIN(rev_timestamp) AS joining_wp_ts," +
//                        "FROM " + util.tableName("bowen_user_dropouts", "revs") +
//                        "GROUP BY user_text, user_id",
//                TableId.of(defaultDataset, "wikipedia_editor_joining_time"));

        // compute the time since editor joining their first project
        util.runQuery("SELECT t1.user_text AS user_text," +
                        "t1.user_id AS user_id," +
                        "t1.joining_wp_ts AS joining_wp_ts," +
                        "t2.joining_project_ts AS joining_project_ts," +
                        "(t2.joining_project_ts - t1.joining_wp_ts) / (3600*24*30) AS tenure_before_projects," +
                        "FROM " + util.tableName(defaultDataset, "wikipedia_editor_joining_time", "t1") +
                        "INNER JOIN " + util.tableName(defaultDataset, "editor_projects_joined_ts_2edits", "t2") +
                        "ON t1.user_text = t2.user_text " +
                        "HAVING tenure_before_projects > 0",
                TableId.of(defaultDataset, "user_wp_project_2edits_joining_ts"));

        // Join tables with the total number of edits in Wikipedia and total tenure length
        util.runQuery("SELECT t1.user_text AS user_text," +
                        "t2.user_id AS user_id," +
                        "t1.projects_joined AS projects_joined," +
                        "t1.total_edits AS total_edits," +
                        "t1.tenure_months AS tenure_months," +
                        "t2.tenure_before_projects AS tenure_before_projects," +
                        "FROM " + util.tableName(defaultDataset, "filtered_users_by_projects_2edits_joining2", "t1") +
                        "INNER JOIN " + util.tableName(defaultDataset, "user_wp_project_2edits_joining_ts", "t2") +
                        "ON t1.user_text = t2.user_text",
                TableId.of(defaultDataset, "filtered_users_by_projects_2edits_joining3"));
    }

    private void statsOnUserEditsBeforeJoiningFirstWikiProject() throws Exception {
        util.runQuery("SELECT user_id," +
                        "user_text," +
                        "COUNT(*) AS total_edits," +
                        "FROM " + util.tableName(defaultDataset, "user_article_edited_ts_before_joining_first_project") +
                        "WHERE user_id != 0 " +
                        "GROUP BY user_id, user_text",
                TableId.of(defaultDataset, "user_total_edits_before_joining_first_project_ns0"));
    }

    private void createCategoryVectorBeforeJoiningFirstWikiProject() throws Exception {

        // edits on articles editors make form joining Wikipedia to first WikiProject
//        util.runQuery("SELECT t1.rev_user_text AS user_text," +
//                        "t1.rev_user_id AS user_id," +
//                        "LOWER(t1.rev_page_title) AS page_title," +
//                        "t1.rev_timestamp AS timestamp," +
//                        "FROM " + util.tableName("bowen_user_dropouts", "revs", "t1") +
//                        "INNER JOIN " + util.tableName(defaultDataset, "editor_projects_joined_ts_2edits", "t2") +
//                        "ON t1.rev_user_text = t2.user_text " +
//                        "WHERE t1.ns = 0 AND t2.projects_joined <= 8 AND t1.rev_timestamp <= t2.joining_project_ts",
//                TableId.of(defaultDataset, "user_article_edited_ts_before_joining_first_project"));

        // TODO: stat check: stats about the number of edits made, and the number of articles edited, etc

        // articles edited for each user
//        util.runQuery("SELECT user_id," +
//                        "user_text," +
//                        "page_title," +
//                        "COUNT(*) AS page_edits," +
//                        "FROM " + util.tableName(defaultDataset, "user_article_edited_ts_before_joining_first_project") +
//                        "GROUP BY user_id, user_text, page_title",
//                TableId.of(defaultDataset, "user_page_edits_count"));
//
//        // join each article with the category info
//        util.runQuery("SELECT t1.user_text AS user_text," +
//                        "t1.user_id AS user_id," +
//                        "t1.page_title AS page_title," +
//                        "t2.super_category AS category," +
//                        "COUNT(*) AS cate_cnt," +
//                        "FROM " + util.tableName(defaultDataset, "user_page_edits_count", "t1") +
//                        "INNER JOIN " + util.tableName("bowen_user_dropouts", "article_categories", "t2") +
//                        "ON t1.page_title = t2.title " +
//                        "GROUP BY user_id, user_text, page_title, category",
//                TableId.of(defaultDataset, "user_article_edited_categories"));

        // Convert long format for wide format
//        util.runQuery("SELECT user_id," +
//                        "user_text," +
//                        "MAX (CASE WHEN category = 'arts' " +
//                        "THEN SUM(cate_cnt) " +
//                        "ELSE 0 " +
//                        "END) AS arts, " +
//                        "MAX (CASE WHEN category = 'geography' " +
//                        "THEN SUM(cate_cnt) " +
//                        "ELSE 0 " +
//                        "END) AS geography, " +
//                        "MAX (CASE WHEN category = 'health' " +
//                        "THEN SUM(cate_cnt) " +
//                        "ELSE 0 " +
//                        "END) AS health, " +
//                        "MAX (CASE WHEN category = 'mathematics' " +
//                        "THEN SUM(cate_cnt) " +
//                        "ELSE 0 " +
//                        "END) AS mathematics, " +
//                        "MAX (CASE WHEN category = 'history' " +
//                        "THEN SUM(cate_cnt) " +
//                        "ELSE 0 " +
//                        "END) AS history, " +
//                        "MAX (CASE WHEN category = 'science' " +
//                        "THEN SUM(cate_cnt) " +
//                        "ELSE 0 " +
//                       "END) AS science, " +
//                        "MAX (CASE WHEN category = 'people' " +
//                        "THEN SUM(cate_cnt) " +
//                        "ELSE 0 " +
//                        "END) AS people, " +
//                        "MAX (CASE WHEN category = 'philosophy' " +
//                        "THEN SUM(cate_cnt) " +
//                        "ELSE 0 " +
//                        "END) AS philosophy, " +
//                        "MAX (CASE WHEN category = 'religion' " +
//                        "THEN SUM(cate_cnt) " +
//                        "ELSE 0 " +
//                        "END) AS religion, " +
//                        "MAX (CASE WHEN category = 'society' " +
//                        "THEN SUM(cate_cnt) " +
//                        "ELSE 0 " +
//                        "END) AS society, " +
//                        "MAX (CASE WHEN category = 'technology' " +
//                        "THEN SUM(cate_cnt) " +
//                        "ELSE 0 " +
//                        "END) AS technology, " +
//                        "MAX (CASE WHEN category = 'Not Found' " +
//                        "THEN SUM(cate_cnt) " +
//                        "ELSE 0 " +
//                        "END) AS NF, " +
//                        "SUM(cate_cnt) AS total," +
//                        "FROM " + util.tableName(defaultDataset, "user_article_edited_categories") +
//                        "GROUP BY user_id, user_text, category",
//                TableId.of(defaultDataset, "user_articles_categories_by_counts"));

//        util.runQuery("SELECT user_id," +
//                        "user_text," +
//                        "SUM(arts) AS arts," +
//                        "SUM(geography) AS geography," +
//                        "SUM(health) AS health," +
//                        "SUM(mathematics) AS mathematics," +
//                        "SUM(history) AS history," +
//                        "SUM(science) AS science," +
//                        "SUM(people) AS people," +
//                        "SUM(philosophy) AS philosophy," +
//                        "SUM(religion) AS religion," +
//                        "SUM(society) AS society," +
//                        "SUM(technology) AS technology," +
//                        "SUM(NF) AS NF," +
//                        "SUM(total) AS total," +
//                        "FROM " + util.tableName(defaultDataset, "user_articles_categories_by_counts") +
////                        "WHERE user_id != 0 AND user_id != null " +
//                        "GROUP BY user_id, user_text ",
//                TableId.of(defaultDataset, "user_category_edit_count_distribution"));
//


//
////        statsOnUserEditsBeforeJoiningFirstWikiProject();
//
//        util.runQuery("SELECT t1.user_id AS user_id," +
//                        "t1.user_text AS user_text," +
//                        "t1.arts AS arts," +
//                        "t1.geography AS geography," +
//                        "t1.health AS health," +
//                        "t1.mathematics AS mathematics," +
//                        "t1.history AS history," +
//                        "t1.science AS science," +
//                        "t1.people AS people," +
//                        "t1.philosophy AS philosophy," +
//                        "t1.religion AS religion," +
//                        "t1.society AS society," +
//                        "t1.technology AS technology," +
//                        "t1.NF AS NF," +
//                        "t2.total_edits AS total_edits," +
//                        "FROM " + util.tableName(defaultDataset, "user_category_distribution_normalized", "t1") +
//                        "INNER JOIN " + util.tableName(defaultDataset, "user_total_edits_before_joining_first_project_ns0", "t2") +
//                        "ON t1.user_id = t2.user_id",
//                TableId.of(defaultDataset, "user_edits_category_distribution_normalized"));

        util.runQuery("SELECT t1.user_id AS user_id," +
//                        "t1.user_text AS user_text," +
                        "t1.arts AS arts," +
                        "t1.geography AS geography," +
                        "t1.health AS health," +
                        "t1.mathematics AS mathematics," +
                        "t1.history AS history," +
                        "t1.science AS science," +
                        "t1.people AS people," +
                        "t1.philosophy AS philosophy," +
                        "t1.religion AS religion," +
                        "t1.society AS society," +
                        "t1.technology AS technology," +
                        "t1.NF AS NF," +
                        "t2.total_edits AS total_edits," +
                        "FROM " + util.tableName(defaultDataset, "user_category_distribution_normalized", "t1") +
                        "INNER JOIN " + util.tableName(defaultDataset, "user_total_edits_before_joining_first_project_ns0", "t2") +
                        "ON t1.user_id = t2.user_id",
                TableId.of(defaultDataset, "user_edits_category_distribution_normalized"));

        // standize project category file
        util.runQuery("SELECT t2.nwikiproject AS nwikiproject," +
                        "t1.arts AS arts," +
                        "t1.geography AS geography," +
                        "t1.health AS health," +
                        "t1.mathematics AS mathematics," +
                        "t1.history AS history," +
                        "t1.science AS science," +
                        "t1.people AS people," +
                        "t1.philosophy AS philosophy," +
                        "t1.religion AS religion," +
                        "t1.society AS society," +
                        "t1.technology AS technology," +
                        "t1.NF AS NF," +
                        "FROM " + util.tableName("bowen_user_dropouts", "wikiproject_category_distribution_normalized", "t1") +
                        "INNER JOIN " + util.tableName("bowen_editor_attachments", "wikiprojects_valid_withIds_3members", "t2") +
                        "ON t1.wikiproject = t2.wikiproject",
                TableId.of(defaultDataset, "project_category_distribution_standardized"));

        // TODO: check distribution of edits

        // cosine similarity between editors and projects
//        util.runQuery("SELECT t1.user_text AS user_text," +
//                        "t1.user_id AS user_id," +
//                        "t1.total_edits AS total_edits," +
//                        "t2.wikiproject AS wikiproject," +
//                        "(t1.arts*t2.arts " +
//                        "+ t1.geography*t2.geography " +
//                        "+ t1.health*t2.health " +
//                        "+ t1.mathematics*t2.mathematics " +
//                        "+ t1.history*t2.history " +
//                        "+ t1.science*t2.science " +
//                        "+ t1.people*t2.people " +
//                        "+ t1.philosophy*t2.philosophy " +
//                        "+ t1.religion*t2.religion " +
//                        "+ t1.society*t2.society " +
//                        "+ t1.technology*t2.technology " +
//                        "+ t1.NF*t2.NF) AS cos_sim," +
//                        "FROM " + util.tableName(defaultDataset, "user_edits_category_distribution_normalized", "t1") +
//                        "CROSS JOIN " + util.tableName("bowen_user_dropouts", "wikiproject_category_distribution_normalized", "t2"),
//                TableId.of(defaultDataset, "user_project_category_cos_sim"));



//        util.runQuery("SELECT *," +
//                        "FROM " + util.tableName(defaultDataset, "user_edits_category_distribution_normalized") +
//                        "LIMIT 10000",
//                TableId.of(defaultDataset, "user_edits_category_distribution_normalized_first10000"));
//
//
//        util.runQuery("SELECT t1.user_id AS user_id," +
//                        "t1.user_text AS user_text," +
//                        "t1.wikiproject AS wikiproject," +
//                        "t1.cos_sim AS cos_sim," +
//                        "t1.total_edits AS total_edits," +
//                        "FROM " + util.tableName(defaultDataset, "user_project_category_cos_sim", "t1") +
//                        "INNER JOIN " + util.tableName(defaultDataset, "user_edits_category_distribution_normalized_first10000", "t2") +
//                        "ON t1.user_id = t2.user_id ",
//                TableId.of(defaultDataset, "user_project_category_cos_sim_first10000"));
//
//        util.runQuery("SELECT *," +
//                        "FROM " + util.tableName(defaultDataset, "user_project_category_cos_sim_first10000") +
//                        "ORDER BY user_id, cos_sim",
//                TableId.of(defaultDataset, "user_project_category_cos_sim_ordered_first10000"));

        /*
        SELECT t1.user_id AS user_id,
t1.user_text As user_text,
t1.wikiproject As wikiproject,
t1.cos_sim AS cos_sim,
FROM WikiProjectRecsys.user_project_category_cos_sim AS t1
JOIN (SELECT MAX(cos_sim) AS cos_sim, user_id,
FROM WikiProjectRecsys.user_project_category_cos_sim
GROUP BY user_id) AS t2
ON t1.user_id = t2.user_id
ORDER BY t2.cos_sim DESC, t1.user_id

         */
//        util.runQuery("SELECT user_id," +
//                                "user_text," +
//                                "wikiproject," +
//                                "cos_sim," +
//                                "FROM " + util.tableName(defaultDataset, "user_project_category_cos_sim") +
//                        "ORDER BY user_id",
//                TableId.of(defaultDataset, "user_project_category_cos_sim_ordered"));
//
//        util.runQuery("SELECT t1.user_id AS user_id," +
//                "t1.user_ext AS user_text," +
//                "t1.total_edits AS total_edits," +
//                "t2." +
//                "FROM " +
//                "INNER JOIN " + util.tableName("bowen_editor_attachments", "dv_user_wikiproject_coordination_edits", "t2") +
//                "ON t1.user_text = t2.user_text");

        // TODO: compute simiarity betwen eidtos and all projects in the database
        // export/read the data into memory and sort projects by editors

        // TODO: rank project-editor pairs by similairty and select top 5/10 projects for each editor
        // export/read the data into memory for the projects they joined.

        // TODO: to double check the intersection of editors

        // TODO: first 1/5 projects editors joined
//        util.runQuery("SELECT u");


    }

    private void createProjectsEditorsJoined() throws Exception {


        // join editors with id
//        util.runQuery("SELECT t2.user_id AS user_id," +
//                        "t1.user_text AS user_text," +
//                        "t1.wikiproject AS wikiproject," +
//                        "t1.total_edits  AS total_edits," +
//                        "t1.first_edit AS first_edit," +
//                        "FROM " + util.tableName("bowen_editor_attachments", "dv_user_wikiproject_coordination_edits", "t1") +
//                        "INNER JOIN " + util.tableName(defaultDataset, "user_category_edit_count_distribution", "t2") +
//                        "ON t1.user_text = t2.user_text",
//                TableId.of(defaultDataset,  "user_id_ns45_edits"));
//
//        // only select editors with less than 8 projects
//        util.runQuery("SELECT t1.user_id AS user_id," +
//                        "t1.user_text AS user_text," +
//                        "t1.total_edits AS total_edits," +
//                        "t1.first_edit AS first_edit," +
//                        "t1.wikiproject AS wikiproject," +
//                        "FROM " + util.tableName(defaultDataset, "user_id_ns45_edits", "t1") +
//                        "INNER JOIN " + util.tableName(defaultDataset, "editor_projects_joined_ts_2edits", "t2") +
//                        "ON t1.user_text = t2.user_text " +
//                        "WHERE t2.projects_joined <= 8 ",
//                TableId.of(defaultDataset, "user_id_more_than_8projects"));
//
//        // join projects with id
//        util.runQuery("SELECT t1.user_id AS user_id," +
//                        "t2.nwikiproject AS nwikiproject," +
//                        "t1.total_edits AS total_edits," +
//                        "t1.first_edit AS first_edit," +
//                        "FROM " + util.tableName(defaultDataset, "user_id_more_than_8projects", "t1") +
//                        "INNER JOIN " + util.tableName("bowen_editor_attachments", "wikiprojects_valid_withIds_3members", "t2") +
//                        "ON t1.wikiproject = t2.wikiproject " +
//                        "WHERE t1.total_edits > 1 " +
//                        "ORDER BY t1.user_id, t1.first_edit",
//                TableId.of(defaultDataset, "final_user_project_ids"));

        // order by first joinning time
        util.runQuery("SELECT *," +
                        "FROM " + util.tableName(defaultDataset, "final_user_project_ids") +
                        "WHERE user_id != 0 " +//AND user_id != null AND user_id != 63 AND user_id != 70 AND user_id != 1982875 AND user_id != 5388617 " +
                        "ORDER BY user_id, first_edit",
                TableId.of(defaultDataset, "user_project_joining_first8"));

    }
}
