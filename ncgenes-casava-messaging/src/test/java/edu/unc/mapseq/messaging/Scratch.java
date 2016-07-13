package edu.unc.mapseq.messaging;

import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.junit.Test;

public class Scratch {

    @Test
    public void testModulus() {

        for (int i = 0; i < 186; ++i) {
            if ((i % 4) == 0) {
                System.out.println(i);
            }
        }

    }

    @Test
    public void asdf() {

        try {
            File sampleSheet = new File("/tmp", "160601_UNC18-D00493_0325_BC8GP3ANXX.csv");
            Reader in = new FileReader(sampleSheet);
            CSVFormat csvFormat = CSVFormat.DEFAULT.withSkipHeaderRecord().withHeader("FCID", "Lane", "SampleID", "SampleRef", "Index",
                    "Description", "Control", "Recipe", "Operator", "SampleProject");
            CSVParser parser = csvFormat.parse(in);
            List<CSVRecord> records = parser.getRecords();
            final Set<String> studyNameSet = new HashSet<>();
            records.forEach(a -> studyNameSet.add(a.get("SampleProject")));
            Collections.synchronizedSet(studyNameSet);

            if (CollectionUtils.isEmpty(studyNameSet)) {
                System.out.println("No Study names in SampleSheet");
            }

            if (studyNameSet.size() > 1) {
                System.out.println("More than one Study in SampleSheet");
            }

            for (CSVRecord record : records) {
                String laneIndex = record.get("Lane");
                System.out.println(laneIndex);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
