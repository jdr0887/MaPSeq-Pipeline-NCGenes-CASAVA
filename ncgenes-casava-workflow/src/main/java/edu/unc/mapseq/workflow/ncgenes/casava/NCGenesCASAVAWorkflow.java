package edu.unc.mapseq.workflow.ncgenes.casava;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang.StringUtils;
import org.jgrapht.DirectedGraph;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.renci.jlrm.condor.CondorJob;
import org.renci.jlrm.condor.CondorJobBuilder;
import org.renci.jlrm.condor.CondorJobEdge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.commons.ncgenes.casava.SaveDemultiplexedStatsAttributesRunnable;
import edu.unc.mapseq.commons.ncgenes.casava.SaveObservedClusterDensityAttributesRunnable;
import edu.unc.mapseq.dao.FlowcellDAO;
import edu.unc.mapseq.dao.MaPSeqDAOBeanService;
import edu.unc.mapseq.dao.MaPSeqDAOException;
import edu.unc.mapseq.dao.model.Attribute;
import edu.unc.mapseq.dao.model.Flowcell;
import edu.unc.mapseq.dao.model.MimeType;
import edu.unc.mapseq.dao.model.Sample;
import edu.unc.mapseq.dao.model.WorkflowRunAttempt;
import edu.unc.mapseq.module.core.CopyFileCLI;
import edu.unc.mapseq.module.core.MakeCLI;
import edu.unc.mapseq.module.core.RemoveCLI;
import edu.unc.mapseq.module.sequencing.casava.ConfigureBCLToFastqCLI;
import edu.unc.mapseq.workflow.WorkflowException;
import edu.unc.mapseq.workflow.impl.AbstractSampleWorkflow;
import edu.unc.mapseq.workflow.impl.WorkflowJobFactory;

public class NCGenesCASAVAWorkflow extends AbstractSampleWorkflow {

    private final Logger logger = LoggerFactory.getLogger(NCGenesCASAVAWorkflow.class);

    public NCGenesCASAVAWorkflow() {
        super();
    }

    @Override
    public String getName() {
        return NCGenesCASAVAWorkflow.class.getSimpleName().replace("Workflow", "");
    }

    @Override
    public String getVersion() {
        ResourceBundle ncgenesBundle = ResourceBundle.getBundle("edu/unc/mapseq/workflow/casava/workflow");
        String version = ncgenesBundle.getString("version");
        return StringUtils.isNotEmpty(version) ? version : "0.0.1-SNAPSHOT";
    }

    @Override
    public Graph<CondorJob, CondorJobEdge> createGraph() throws WorkflowException {
        logger.debug("ENTERING createGraph()");

        DirectedGraph<CondorJob, CondorJobEdge> graph = new DefaultDirectedGraph<CondorJob, CondorJobEdge>(CondorJobEdge.class);

        int count = 0;

        WorkflowRunAttempt attempt = getWorkflowRunAttempt();

        try {

            List<Flowcell> flowcellList = getWorkflowBeanService().getMaPSeqDAOBeanService().getFlowcellDAO()
                    .findByWorkflowRunId(attempt.getWorkflowRun().getId());

            if (flowcellList != null && !flowcellList.isEmpty()) {
                for (Flowcell flowcell : flowcellList) {

                    File baseDir = new File(flowcell.getBaseDirectory());
                    File flowcellDir = new File(baseDir, flowcell.getName());
                    File dataDir = new File(flowcellDir, "Data");
                    File intensitiesDir = new File(dataDir, "Intensities");
                    File baseCallsDir = new File(intensitiesDir, "BaseCalls");

                    List<Sample> sampleList = getWorkflowBeanService().getMaPSeqDAOBeanService().getSampleDAO()
                            .findByFlowcellId(flowcell.getId());

                    Map<Integer, List<Sample>> laneMap = new HashMap<Integer, List<Sample>>();

                    if (sampleList != null && !sampleList.isEmpty()) {

                        logger.info("sampleList.size() = {}", sampleList.size());

                        for (Sample sample : sampleList) {
                            if (laneMap.containsKey(sample.getLaneIndex()) || "Undetermined".equals(sample.getBarcode())) {
                                continue;
                            }
                            laneMap.put(sample.getLaneIndex(), new ArrayList<Sample>());
                        }

                        for (Sample sample : sampleList) {
                            if ("Undetermined".equals(sample.getBarcode())) {
                                continue;
                            }
                            laneMap.get(sample.getLaneIndex()).add(sample);
                        }
                    }

                    if (laneMap.size() > 0) {

                        String siteName = getWorkflowBeanService().getAttributes().get("siteName");

                        for (Integer laneIndex : laneMap.keySet()) {

                            try {

                                File unalignedDir = new File(flowcellDir, String.format("%s.%d", "Unaligned", laneIndex));

                                Set<Attribute> flowcellAttributeSet = flowcell.getAttributes();

                                File sampleSheetFile = null;
                                Integer readCount = null;

                                if (flowcellAttributeSet != null) {
                                    Iterator<Attribute> flowcellAttributeIter = flowcellAttributeSet.iterator();
                                    while (flowcellAttributeIter.hasNext()) {
                                        Attribute ea = flowcellAttributeIter.next();
                                        if ("sampleSheet".equals(ea.getName())) {
                                            sampleSheetFile = new File(ea.getValue());
                                        }
                                        if ("readCount".equals(ea.getName())) {
                                            readCount = Integer.valueOf(ea.getValue());
                                        }
                                    }
                                }

                                if (sampleSheetFile == null) {
                                    sampleSheetFile = new File(baseCallsDir, "SampleSheet.csv");
                                }

                                if (!sampleSheetFile.exists()) {
                                    logger.error("Specified sample sheet doesn't exist: {}", sampleSheetFile.getAbsolutePath());
                                    throw new WorkflowException("Invalid SampleSheet: ");
                                }

                                CondorJobBuilder builder = WorkflowJobFactory
                                        .createJob(++count, ConfigureBCLToFastqCLI.class, attempt.getId()).siteName(siteName);
                                builder.addArgument(ConfigureBCLToFastqCLI.INPUTDIR, baseCallsDir.getAbsolutePath())
                                        .addArgument(ConfigureBCLToFastqCLI.MISMATCHES).addArgument(ConfigureBCLToFastqCLI.IGNOREMISSINGBCL)
                                        .addArgument(ConfigureBCLToFastqCLI.IGNOREMISSINGSTATS)
                                        .addArgument(ConfigureBCLToFastqCLI.FASTQCLUSTERCOUNT, "0")
                                        .addArgument(ConfigureBCLToFastqCLI.TILES, laneIndex.toString())
                                        .addArgument(ConfigureBCLToFastqCLI.OUTPUTDIR, unalignedDir.getAbsolutePath())
                                        .addArgument(ConfigureBCLToFastqCLI.SAMPLESHEET, sampleSheetFile.getAbsolutePath())
                                        .addArgument(ConfigureBCLToFastqCLI.FORCE);
                                CondorJob configureBCLToFastQJob = builder.build();
                                logger.info(configureBCLToFastQJob.toString());
                                graph.addVertex(configureBCLToFastQJob);

                                if (unalignedDir.exists()) {
                                    builder = WorkflowJobFactory.createJob(++count, RemoveCLI.class, attempt.getId()).siteName(siteName);
                                    builder.addArgument(RemoveCLI.FILE, unalignedDir);
                                    CondorJob removeUnalignedDirectoryJob = builder.build();
                                    logger.info(removeUnalignedDirectoryJob.toString());
                                    graph.addVertex(removeUnalignedDirectoryJob);
                                    graph.addEdge(removeUnalignedDirectoryJob, configureBCLToFastQJob);
                                }

                                builder = WorkflowJobFactory.createJob(++count, MakeCLI.class, attempt.getId()).siteName(siteName)
                                        .numberOfProcessors(2);
                                builder.addArgument(MakeCLI.THREADS, "2").addArgument(MakeCLI.WORKDIR, unalignedDir.getAbsolutePath());
                                CondorJob makeJob = builder.build();
                                logger.info(makeJob.toString());
                                graph.addVertex(makeJob);
                                graph.addEdge(configureBCLToFastQJob, makeJob);

                                logger.debug("readCount = {}", readCount);

                                CondorJob copyRead1Job = null;
                                CondorJob copyRead2Job = null;
                                for (Sample sample : laneMap.get(laneIndex)) {

                                    File outputDirectory = new File(sample.getOutputDirectory(), getName());
                                    File tmpDirectory = new File(outputDirectory, "tmp");
                                    tmpDirectory.mkdirs();

                                    logger.info("outputDirectory.getAbsolutePath(): {}", outputDirectory.getAbsolutePath());

                                    File projectDirectory = new File(unalignedDir, "Project_" + sample.getStudy().getName());
                                    File sampleDirectory = new File(projectDirectory, "Sample_" + sample.getName());

                                    File sourceFile = null;
                                    File outputFile = null;
                                    String outputFileName = null;

                                    switch (readCount) {
                                        case 1:
                                            builder = WorkflowJobFactory
                                                    .createJob(++count, CopyFileCLI.class, attempt.getId(), sample.getId())
                                                    .siteName(siteName);
                                            sourceFile = new File(sampleDirectory, String.format("%s_%s_L%03d_R%d_001.fastq.gz",
                                                    sample.getName(), sample.getBarcode(), laneIndex, 1));
                                            outputFileName = String.format("%s_%s_L%03d_R%d.fastq.gz", flowcell.getName(),
                                                    sample.getBarcode(), laneIndex, 1);
                                            outputFile = new File(outputDirectory, outputFileName);
                                            builder.addArgument(CopyFileCLI.SOURCE, sourceFile.getAbsolutePath())
                                                    .addArgument(CopyFileCLI.DESTINATION, outputFile.getAbsolutePath())
                                                    .addArgument(CopyFileCLI.MIMETYPE, MimeType.FASTQ.toString());
                                            copyRead1Job = builder.build();
                                            logger.info(copyRead1Job.toString());
                                            graph.addVertex(copyRead1Job);
                                            graph.addEdge(makeJob, copyRead1Job);

                                            break;
                                        case 2:
                                        default:

                                            // read 1
                                            builder = WorkflowJobFactory
                                                    .createJob(++count, CopyFileCLI.class, attempt.getId(), sample.getId())
                                                    .siteName(siteName);
                                            sourceFile = new File(sampleDirectory, String.format("%s_%s_L%03d_R%d_001.fastq.gz",
                                                    sample.getName(), sample.getBarcode(), laneIndex, 1));
                                            outputFileName = String.format("%s_%s_L%03d_R%d.fastq.gz", flowcell.getName(),
                                                    sample.getBarcode(), laneIndex, 1);
                                            outputFile = new File(outputDirectory, outputFileName);
                                            builder.addArgument(CopyFileCLI.SOURCE, sourceFile.getAbsolutePath())
                                                    .addArgument(CopyFileCLI.DESTINATION, outputFile.getAbsolutePath())
                                                    .addArgument(CopyFileCLI.MIMETYPE, MimeType.FASTQ.toString());
                                            copyRead1Job = builder.build();
                                            logger.info(copyRead1Job.toString());
                                            graph.addVertex(copyRead1Job);
                                            graph.addEdge(makeJob, copyRead1Job);

                                            // read 2
                                            builder = WorkflowJobFactory
                                                    .createJob(++count, CopyFileCLI.class, attempt.getId(), sample.getId())
                                                    .siteName(siteName);
                                            sourceFile = new File(sampleDirectory, String.format("%s_%s_L%03d_R%d_001.fastq.gz",
                                                    sample.getName(), sample.getBarcode(), laneIndex, 2));
                                            outputFileName = String.format("%s_%s_L%03d_R%d.fastq.gz", flowcell.getName(),
                                                    sample.getBarcode(), laneIndex, 2);
                                            outputFile = new File(outputDirectory, outputFileName);
                                            builder.addArgument(CopyFileCLI.SOURCE, sourceFile.getAbsolutePath())
                                                    .addArgument(CopyFileCLI.DESTINATION, outputFile.getAbsolutePath())
                                                    .addArgument(CopyFileCLI.MIMETYPE, MimeType.FASTQ.toString());
                                            copyRead2Job = builder.build();
                                            logger.info(copyRead2Job.toString());
                                            graph.addVertex(copyRead2Job);
                                            graph.addEdge(makeJob, copyRead2Job);

                                            break;
                                    }

                                }

                                builder = WorkflowJobFactory.createJob(++count, RemoveCLI.class, attempt.getId()).siteName(siteName);
                                builder.addArgument(RemoveCLI.FILE, unalignedDir);
                                CondorJob removeUnalignedDirectoryJob = builder.build();
                                logger.info(removeUnalignedDirectoryJob.toString());
                                graph.addVertex(removeUnalignedDirectoryJob);
                                graph.addEdge(removeUnalignedDirectoryJob, copyRead1Job);
                                if (copyRead2Job != null) {
                                    graph.addEdge(removeUnalignedDirectoryJob, copyRead2Job);
                                }

                            } catch (Exception e) {
                                throw new WorkflowException(e);
                            }

                        }

                    }

                }
            }
        } catch (MaPSeqDAOException e) {
            throw new WorkflowException(e);
        }

        return graph;
    }

    @Override
    public void postRun() throws WorkflowException {

        MaPSeqDAOBeanService maPSeqDAOBeanService = getWorkflowBeanService().getMaPSeqDAOBeanService();
        FlowcellDAO flowcellDAO = maPSeqDAOBeanService.getFlowcellDAO();
        List<Long> flowcellIdList = new ArrayList<Long>();

        WorkflowRunAttempt attempt = getWorkflowRunAttempt();

        try {
            List<Flowcell> flowcellList = flowcellDAO.findByWorkflowRunId(attempt.getWorkflowRun().getId());

            if (flowcellList != null && !flowcellList.isEmpty()) {
                for (Flowcell flowcell : flowcellList) {
                    flowcellIdList.add(flowcell.getId());
                }
            }
        } catch (MaPSeqDAOException e) {
            e.printStackTrace();
        }

        ExecutorService executorService = Executors.newSingleThreadExecutor();

        if (flowcellIdList != null && !flowcellIdList.isEmpty()) {

            // FixMismappedFastqFileDataRunnable fixMismappedFastqFileDataRunnable = new
            // FixMismappedFastqFileDataRunnable();
            // fixMismappedFastqFileDataRunnable.setMapseqDAOBean(getWorkflowBeanService().getMaPSeqDAOBean());
            // fixMismappedFastqFileDataRunnable.setFlowcellIdList(flowcellIdList);
            // executorService.submit(fixMismappedFastqFileDataRunnable);

            SaveDemultiplexedStatsAttributesRunnable saveDemultiplexedStatsAttributesRunnable = new SaveDemultiplexedStatsAttributesRunnable();
            saveDemultiplexedStatsAttributesRunnable.setMaPSeqDAOBeanService(getWorkflowBeanService().getMaPSeqDAOBeanService());
            saveDemultiplexedStatsAttributesRunnable.setFlowcellIdList(flowcellIdList);
            executorService.submit(saveDemultiplexedStatsAttributesRunnable);

            SaveObservedClusterDensityAttributesRunnable saveObservedClusterDensityAttributesRunnable = new SaveObservedClusterDensityAttributesRunnable();
            saveObservedClusterDensityAttributesRunnable.setMaPSeqDAOBeanService(getWorkflowBeanService().getMaPSeqDAOBeanService());
            saveObservedClusterDensityAttributesRunnable
                    .setMaPSeqConfigurationService(getWorkflowBeanService().getMaPSeqConfigurationService());
            saveObservedClusterDensityAttributesRunnable.setFlowcellIdList(flowcellIdList);
            executorService.submit(saveObservedClusterDensityAttributesRunnable);

        }

        executorService.shutdown();
    }

}
