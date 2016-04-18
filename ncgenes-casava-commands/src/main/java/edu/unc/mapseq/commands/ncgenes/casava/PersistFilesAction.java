package edu.unc.mapseq.commands.ncgenes.casava;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.karaf.shell.api.action.Action;
import org.apache.karaf.shell.api.action.Argument;
import org.apache.karaf.shell.api.action.Command;
import org.apache.karaf.shell.api.action.lifecycle.Reference;
import org.apache.karaf.shell.api.action.lifecycle.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.dao.MaPSeqDAOBeanService;
import edu.unc.mapseq.dao.MaPSeqDAOException;
import edu.unc.mapseq.dao.model.FileData;
import edu.unc.mapseq.dao.model.MimeType;
import edu.unc.mapseq.dao.model.Sample;

@Command(scope = "ncgenes-casava", name = "persist-files", description = "Persist Files")
@Service
public class PersistFilesAction implements Action {

    private static final Logger logger = LoggerFactory.getLogger(PersistFilesAction.class);

    @Reference
    private MaPSeqDAOBeanService maPSeqDAOBeanService;

    @Argument(index = 0, name = "sampleId", required = true, multiValued = true)
    private List<Long> sampleIdList;

    public PersistFilesAction() {
        super();
    }

    @Override
    public Object execute() {
        logger.debug("ENTERING execute()");

        try {

            if (CollectionUtils.isNotEmpty(sampleIdList)) {
                for (Long sampleId : sampleIdList) {

                    Sample sample = maPSeqDAOBeanService.getSampleDAO().findById(sampleId);

                    File workflowDirectory = new File(sample.getOutputDirectory(), "NCGenesCASAVA");

                    Set<File> expectedFileSet = new HashSet<File>();

                    expectedFileSet.add(new File(workflowDirectory, String.format("%s_%s_L%03d_R%d.fastq.gz",
                            sample.getFlowcell().getName(), sample.getBarcode(), sample.getLaneIndex(), 1)));

                    expectedFileSet.add(new File(workflowDirectory, String.format("%s_%s_L%03d_R%d.fastq.gz",
                            sample.getFlowcell().getName(), sample.getBarcode(), sample.getLaneIndex(), 2)));

                    Set<FileData> fileDataSet = sample.getFileDatas();
                    if (CollectionUtils.isEmpty(fileDataSet)) {
                        for (File file : expectedFileSet) {
                            if (file.exists()) {
                                FileData fileData = new FileData(file.getName(), workflowDirectory.getAbsolutePath(), MimeType.FASTQ);
                                List<FileData> foundFileDataList = maPSeqDAOBeanService.getFileDataDAO().findByExample(fileData);
                                if (CollectionUtils.isNotEmpty(foundFileDataList)) {
                                    fileData = foundFileDataList.get(0);
                                } else {
                                    fileData.setId(maPSeqDAOBeanService.getFileDataDAO().save(fileData));
                                }
                                sample.getFileDatas().add(fileData);
                            }
                        }
                        maPSeqDAOBeanService.getSampleDAO().save(sample);
                    }

                    if (CollectionUtils.isNotEmpty(fileDataSet)) {

                        Set<File> fileSet = new HashSet<>();
                        fileDataSet.forEach(a -> fileSet.add(a.toFile()));

                        if (!fileDataSet.containsAll(expectedFileSet)) {

                            for (File file : expectedFileSet) {
                                if (file.exists()) {
                                    FileData fileData = new FileData(file.getName(), workflowDirectory.getAbsolutePath(), MimeType.FASTQ);
                                    List<FileData> foundFileDataList = maPSeqDAOBeanService.getFileDataDAO().findByExample(fileData);
                                    if (CollectionUtils.isNotEmpty(foundFileDataList)) {
                                        fileData = foundFileDataList.get(0);
                                    } else {
                                        fileData.setId(maPSeqDAOBeanService.getFileDataDAO().save(fileData));
                                    }
                                    sample.getFileDatas().add(fileData);
                                }
                            }
                            maPSeqDAOBeanService.getSampleDAO().save(sample);

                        }

                    }

                }
            }

        } catch (MaPSeqDAOException e) {
            e.printStackTrace();
        }

        return null;
    }

    public List<Long> getSampleIdList() {
        return sampleIdList;
    }

    public void setSampleIdList(List<Long> sampleIdList) {
        this.sampleIdList = sampleIdList;
    }

}
