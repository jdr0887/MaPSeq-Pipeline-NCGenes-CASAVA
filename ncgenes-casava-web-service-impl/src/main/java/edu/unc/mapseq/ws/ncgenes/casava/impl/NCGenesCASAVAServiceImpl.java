package edu.unc.mapseq.ws.ncgenes.casava.impl;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import javax.activation.DataHandler;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.dao.FileDataDAO;
import edu.unc.mapseq.dao.MaPSeqDAOException;
import edu.unc.mapseq.dao.model.FileData;
import edu.unc.mapseq.dao.model.MimeType;
import edu.unc.mapseq.ws.ncgenes.casava.NCGenesCASAVAService;

public class NCGenesCASAVAServiceImpl implements NCGenesCASAVAService {

    private static final Logger logger = LoggerFactory.getLogger(NCGenesCASAVAServiceImpl.class);

    private FileDataDAO fileDataDAO;

    @Override
    public Long uploadSampleSheet(DataHandler data, String flowcellName) {
        logger.debug("ENTERING upload(Holder<DataHandler>)");
        FileData fileData = null;
        try {

            File file = new File("/projects/sequence_analysis/medgenwork/prod/NC_GENES/SampleSheets",
                    String.format("%s.csv", flowcellName));
            logger.info("file.getAbsolutePath(): {}", file.getAbsolutePath());

            InputStream is = data.getInputStream();
            FileOutputStream fos = new FileOutputStream(file);
            IOUtils.copyLarge(is, fos);
            is.close();
            fos.flush();
            fos.close();

            fileData = new FileData();
            fileData.setMimeType(MimeType.TEXT_CSV);
            fileData.setName(file.getName());
            fileData.setPath(file.getParentFile().getAbsolutePath());

            List<FileData> fileDataList = fileDataDAO.findByExample(fileData);
            if (fileDataList != null && fileDataList.size() > 0) {
                fileData = fileDataList.get(0);
            }

            if (fileDataList == null || (fileDataList != null && fileDataList.size() == 0)) {
                Long id = fileDataDAO.save(fileData);
                fileData.setId(id);
            }

        } catch (MaPSeqDAOException | IOException e) {
            logger.error(e.getMessage(), e);
        }
        return fileData.getId();
    }

    @Override
    public Boolean assertDirectoryExists(String studyName, String flowcell) {
        logger.debug("ENTERING assertDirectoryExists(String, String)");
        String mapseqBaseDirectory = System.getenv("MAPSEQ_BASE_DIRECTORY");
        File studyDir = new File(mapseqBaseDirectory, studyName);
        File outDir = new File(studyDir, "out");
        File sequencerRunDir = new File(outDir, flowcell);
        if (sequencerRunDir.exists()) {
            return true;
        }
        return false;
    }

    public FileDataDAO getFileDataDAO() {
        return fileDataDAO;
    }

    public void setFileDataDAO(FileDataDAO fileDataDAO) {
        this.fileDataDAO = fileDataDAO;
    }

}
