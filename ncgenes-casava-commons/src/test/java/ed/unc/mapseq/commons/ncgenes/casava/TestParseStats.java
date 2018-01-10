package ed.unc.mapseq.commons.ncgenes.casava;

import java.io.File;
import java.util.Iterator;

import org.apache.commons.io.FileUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.junit.Test;

public class TestParseStats {


    @Test
    public void testUsingHTMLSource() throws Exception {

        File statsFile = new File(
                "/home/jdr0887/Downloads/171218_UNC31-K00269_0104_AHNCH5BBXX/4/Reports/html/HNCH5BBXX/NC_GENES/FES_0027_0/ATCACG",
                "laneBarcode.html");

        org.jsoup.nodes.Document doc = Jsoup.parse(FileUtils.readFileToString(statsFile));
        Iterator<Element> tableIter = doc.select("table").iterator();
        tableIter.next();
        tableIter.next();

        for (Element row : tableIter.next().select("tr")) {

            Elements elements = row.select("td");

            if (elements.isEmpty()) {
                continue;
            }

            Iterator<Element> tdIter = elements.iterator();

            Element laneElement = tdIter.next();
            Element passingFilteringElement = tdIter.next();
            Element percentOfTheLaneElement = tdIter.next();
            Element percentOfTheBarcodeElement = tdIter.next();
            Element percentOneMismatchElement = tdIter.next();
            Element yeildElement = tdIter.next();
            Element percentPassingFilteringClustersElement = tdIter.next();
            Element q30YeildElement = tdIter.next();
            Element meanQualityScoreElement = tdIter.next();
            
            System.out.println("");
        }

    }

}
