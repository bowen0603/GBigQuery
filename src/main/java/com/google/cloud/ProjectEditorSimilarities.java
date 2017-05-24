package com.google.cloud;

import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by bobo on 5/21/17.
 * TODO: check the popularity distribution of recommended projects
 * read recommendations from files instead, no need to recompute every time
 */
public class ProjectEditorSimilarities {

    private String editorCategoryInfoFile;
    private String projectCategoryInfoFile;
    private String editorTopNRecommendations;
    private String projectsEditorsJoined;
    private String cvsSplitBy;
    private int topN;

    private Map<String, Map<String, Double>> editorsTopNRecommendations;
    private Map<String, List<String>> editorProjectsJoined;
    private Map<String, String> bestPredictions;

    public ProjectEditorSimilarities() {

        editorCategoryInfoFile = "data/user_category.csv";
        projectCategoryInfoFile = "data/wikiproject_category.csv";
        editorTopNRecommendations = "data/topNRecommendations.csv";
        projectsEditorsJoined = "data/first8projects_joined.csv";
        cvsSplitBy = ",";
        topN = 5;

        editorsTopNRecommendations = new HashMap<>();
        editorProjectsJoined = new HashMap<>();
        bestPredictions = new HashMap<>();
    }

    public static void main(String... args) throws Exception {
        ProjectEditorSimilarities self = new ProjectEditorSimilarities();

        long startTime = System.currentTimeMillis();

        self.readEditorProjectCategoryPreference();

        self.readProjectEditorsJoined();

        self.comparePredictionAccuracy();

        long endTime   = System.currentTimeMillis();


        System.out.println(TimeUnit.MILLISECONDS.toMinutes(endTime - startTime) + " minutes..");

    }

    /**
     * Compute the topN recommendation projects for each editor, and store them in a dictionary
     */
    private void readEditorProjectCategoryPreference() {
        BufferedReader brEditors=null, brProjects=null;
        BufferedWriter bwEdistors=null;

        System.out.println("Working on Editor's top N recommendations..");

        try {
            int editorIdx = 0;
            String editorID=null, projectName=null;
            String sEditorCurrentLine, sProjectCurrentLine;

            brEditors = new BufferedReader(new FileReader(editorCategoryInfoFile));
            bwEdistors = new BufferedWriter(new FileWriter(editorTopNRecommendations));
            bwEdistors.write("editorID,rank,project,simScore\n");



            boolean editorFileHeader = true;
            while ((sEditorCurrentLine = brEditors.readLine()) != null) {

//                if (editorIdx % 4000 == 0) {
//                    System.out.println((editorIdx++ / 4000) + "% processed..");
//                }

//                if (editorIdx++ > 10) {
//                    break;
//                }

                if (editorFileHeader) {
                    editorFileHeader = false;
                    continue;
                }

                // read editor category info
                Map<String, Double> editorTopNRecommendations = new HashMap<>();
                String[] editorItems = sEditorCurrentLine.split(cvsSplitBy);
                editorID = editorItems[0];

//                System.out.println(editorIdx++ + ". Working on Editor: " + editorID);

                boolean projectFileHeader = true;
                brProjects = new BufferedReader(new FileReader(projectCategoryInfoFile));

                while ((sProjectCurrentLine = brProjects.readLine()) != null) {
                    if (projectFileHeader) {
                        projectFileHeader = false;
                        continue;
                    }

                    // read project category info
                    String[] projectItems = sProjectCurrentLine.split(cvsSplitBy);
                    projectName = projectItems[0];

                    // compute cosine similarity
                    double cosSimValue = computeCosineSimilarity(editorItems, projectItems);

                    // update editor's top N recommendations
                    insertEditorTopNProjects(projectName, cosSimValue, editorTopNRecommendations);
                }

                editorsTopNRecommendations.put(editorID, editorTopNRecommendations);
                writeEditorRecommendationsToFile(bwEdistors, editorID, editorTopNRecommendations);
            }
        } catch (IOException e) {

            e.printStackTrace();
        } finally {

            try {
                if (brEditors != null)
                    brEditors.close();
                if (brProjects != null)
                    brProjects.close();
                if (bwEdistors != null)
                    bwEdistors.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }

        }

    }

    private void writeEditorRecommendationsToFile(BufferedWriter bw, String editorID, Map<String, Double> recommendations) {
        // Get entries and sort them.
        List<Map.Entry<String, Double>> entries = new ArrayList<Map.Entry<String, Double>>(recommendations.entrySet());
        Collections.sort(entries, new Comparator<Map.Entry<String, Double>>() {
            public int compare(Map.Entry<String, Double> e1, Map.Entry<String, Double> e2) {
                return e2.getValue().compareTo(e1.getValue());
            }
        });

        // Put entries back in an ordered map.
        Map<String, Double> orderedMap = new LinkedHashMap<String, Double>();
        for (Map.Entry<String, Double> entry : entries) {
            orderedMap.put(entry.getKey(), entry.getValue());
        }

//        System.out.println(orderedMap); // {foo=bar, waa=foo, bar=waa}
        int cnt = 1;
        String bestPrediction = "";
        double maxVal = -1;
        for (String iproject: orderedMap.keySet()) {
            try {
                bw.write(editorID+","+(cnt++)+","+iproject+","+orderedMap.get(iproject)+"\n");
            } catch (IOException e) {
                e.printStackTrace();
            }
            if (orderedMap.get(iproject) >  maxVal) {
                maxVal = orderedMap.get(iproject);
                bestPrediction = iproject;
            }
        }
        bestPredictions.put(editorID, bestPrediction);
    }

    private double computeCosineSimilarity(String[] editorItems, String[] projectItems) {
        // TODO: make sure the orders are correct for cos compute

        double simVal = 0;

        // arts
        simVal += Double.parseDouble(editorItems[1]) * Double.parseDouble(projectItems[1]);
        // geography
        simVal += Double.parseDouble(editorItems[2]) * Double.parseDouble(projectItems[2]);
        // health
        simVal += Double.parseDouble(editorItems[3]) * Double.parseDouble(projectItems[3]);
        // mathematics
        simVal += Double.parseDouble(editorItems[4]) * Double.parseDouble(projectItems[4]);
        // history
        simVal += Double.parseDouble(editorItems[5]) * Double.parseDouble(projectItems[5]);
        // science
        simVal += Double.parseDouble(editorItems[6]) * Double.parseDouble(projectItems[6]);
        // people
        simVal += Double.parseDouble(editorItems[7]) * Double.parseDouble(projectItems[7]);
        // philosophy
        simVal += Double.parseDouble(editorItems[8]) * Double.parseDouble(projectItems[8]);
        // religion
        simVal += Double.parseDouble(editorItems[9]) * Double.parseDouble(projectItems[9]);
        // society
        simVal += Double.parseDouble(editorItems[10]) * Double.parseDouble(projectItems[10]);
        // technology
        simVal += Double.parseDouble(editorItems[11]) * Double.parseDouble(projectItems[11]);
        // NF
        simVal += Double.parseDouble(editorItems[12]) * Double.parseDouble(projectItems[12]);

        return simVal;
    }

    private void insertEditorTopNProjects(String projectName, double simVal, Map<String, Double> editorTopN) {

        if (editorTopN.size() < topN) {
            editorTopN.put(projectName, simVal);
        } else {
            String minProject = null;
            double minVal = 10;

            for (String iProject: editorTopN.keySet()) {
                // TODO: potential operations for tie breaking
                if (editorTopN.get(iProject) < minVal) {
                    minProject = iProject;
                    minVal = editorTopN.get(iProject);
                }
            }

            // if the new project is in top N
            if (minVal < simVal) {
                editorTopN.remove(minProject);
                editorTopN.put(projectName, simVal);
            }
        }

    }

    private void readProjectEditorsJoined() {
        String preEditorID = "-1";
        BufferedReader brEditors=null;

        System.out.println("Reading Projects Editors Joined ..");

        try {
            List<String> projectList = new LinkedList<>();
            String sEditorCurrentLine;

            boolean projectFileHeader = true;
            brEditors = new BufferedReader(new FileReader(projectsEditorsJoined));



            while ((sEditorCurrentLine = brEditors.readLine()) != null) {
                if (projectFileHeader) {
                    projectFileHeader = false;
                    continue;
                }

                // read project category info
                String[] editorItems = sEditorCurrentLine.split(cvsSplitBy);
//                int editorID = Integer.parseInt(editorItems[0]);
//                int projectID = Integer.parseInt(editorItems[1]);

                String editorID = editorItems[0];
                String projectID = editorItems[1];

                if (editorID.equals(preEditorID)) {
                    projectList.add(projectID);
                } else {
                    if (!preEditorID.equals("-1")) {
                        editorProjectsJoined.put(editorID, new LinkedList<String>(projectList));
                        projectList.clear();
                    }
                    preEditorID =  editorID;
                    projectList.add(projectID);
                }

            }

        } catch (IOException e) {

            e.printStackTrace();
        } finally {

            try {
                if (brEditors != null)
                    brEditors.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }

        }
    }

    private void comparePredictionAccuracy() {

        int correctFirstPredictions=0, correctPredictions=0, totalPredictions=0;
        int predictedEditors=0, totalEditors=editorProjectsJoined.size();

        for (String ieditor: editorProjectsJoined.keySet()) {
            if (!editorsTopNRecommendations.containsKey(ieditor)) {
                System.out.println("Editor " + ieditor + " was not recommended..");
                continue;
            }

            Map<String, Double> recommendations = editorsTopNRecommendations.get(ieditor);

            boolean correctPrediction = false;
            for (String predProject : editorProjectsJoined.get(ieditor)) {
                if (recommendations.containsKey(predProject)) {
                    ++correctPredictions;
                    correctPrediction = true;
                }
            }
            if (correctPrediction) {
                ++predictedEditors;
            }

            // prediction on the first project
            if (editorProjectsJoined.get(ieditor).contains(bestPredictions.get(ieditor))) {
                ++correctFirstPredictions;
            }
            totalPredictions += topN;
        }

        System.out.println(correctPredictions + " out of " + totalPredictions + " predictions were correct, " + (correctPredictions/totalPredictions));
        System.out.println(predictedEditors + " out of " + totalEditors + " editors got correct predictions @"+topN +", " + (predictedEditors/totalEditors));
        System.out.println(correctFirstPredictions + " out of " + totalEditors + " editors got first correct predictions @1," + (predictedEditors/totalEditors));
    }


}


class ValueComparator implements Comparator<String> {
    Map<String, Double> base;

    public ValueComparator(Map<String, Double> base) {
        this.base = base;
    }

    // Note: this comparator imposes orderings that are inconsistent with
    // equals.
    public int compare(String a, String b) {
        if (base.get(a) >= base.get(b)) {
            return -1;
        } else {
            return 1;
        } // returning 0 would merge keys
    }
}