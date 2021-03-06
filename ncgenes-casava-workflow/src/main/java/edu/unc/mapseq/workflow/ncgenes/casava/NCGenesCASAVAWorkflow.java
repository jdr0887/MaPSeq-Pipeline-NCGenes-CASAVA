package edu.unc.mapseq.workflow.ncgenes.casava;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.jgrapht.DirectedGraph;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.renci.jlrm.condor.CondorJob;
import org.renci.jlrm.condor.CondorJobBuilder;
import org.renci.jlrm.condor.CondorJobEdge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.commons.ncgenes.casava.FindReadCountCallable;
import edu.unc.mapseq.commons.ncgenes.casava.RegisterToIRODSRunnable;
import edu.unc.mapseq.commons.ncgenes.casava.SaveDemultiplexedStatsAttributesRunnable;
import edu.unc.mapseq.dao.MaPSeqDAOBeanService;
import edu.unc.mapseq.dao.MaPSeqDAOException;
import edu.unc.mapseq.dao.model.Attribute;
import edu.unc.mapseq.dao.model.FileData;
import edu.unc.mapseq.dao.model.Flowcell;
import edu.unc.mapseq.dao.model.MimeType;
import edu.unc.mapseq.dao.model.Sample;
import edu.unc.mapseq.dao.model.WorkflowRun;
import edu.unc.mapseq.dao.model.WorkflowRunAttempt;
import edu.unc.mapseq.module.core.CopyDirectoryCLI;
import edu.unc.mapseq.module.core.CopyFile2CLI;
import edu.unc.mapseq.module.core.RemoveCLI;
import edu.unc.mapseq.module.sequencing.bcl2fastq.BCL2FastqCLI;
import edu.unc.mapseq.workflow.WorkflowException;
import edu.unc.mapseq.workflow.core.WorkflowJobFactory;
import edu.unc.mapseq.workflow.sequencing.AbstractSequencingWorkflow;
import edu.unc.mapseq.workflow.sequencing.SequencingWorkflowJobFactory;
import edu.unc.mapseq.workflow.sequencing.SequencingWorkflowUtil;

public class NCGenesCASAVAWorkflow extends AbstractSequencingWorkflow {

    private static final Logger logger = LoggerFactory.getLogger(NCGenesCASAVAWorkflow.class);

    public NCGenesCASAVAWorkflow() {
        super();
    }

    @Override
    public Graph<CondorJob, CondorJobEdge> createGraph() throws WorkflowException {
        logger.debug("ENTERING createGraph()");

        DirectedGraph<CondorJob, CondorJobEdge> graph = new DefaultDirectedGraph<CondorJob, CondorJobEdge>(CondorJobEdge.class);

        int count = 0;

        WorkflowRunAttempt attempt = getWorkflowRunAttempt();
        WorkflowRun workflowRun = attempt.getWorkflowRun();

        boolean allowMismatches = true;

        String siteName = getWorkflowBeanService().getAttributes().get("siteName");
        String flowcellStagingDirectory = getWorkflowBeanService().getAttributes().get("flowcellStagingDirectory");

        Set<Attribute> workflowRunAttributeList = workflowRun.getAttributes();
        for (Attribute attribute : workflowRunAttributeList) {
            if (attribute.getName().equals("allowMismatches") && attribute.getValue().equalsIgnoreCase("false")) {
                allowMismatches = false;
            }
        }

        try {

            List<Flowcell> flowcellList = getWorkflowBeanService().getMaPSeqDAOBeanService().getFlowcellDAO()
                    .findByWorkflowRunId(workflowRun.getId());

            if (CollectionUtils.isEmpty(flowcellList)) {
                throw new WorkflowException("No Flowcells to process found");
            }

            for (Flowcell flowcell : flowcellList) {

                File sampleSheetFile = null;
                FileData flowcellFileData = flowcell.getFileDatas().stream()
                        .filter(a -> a.getName().equals(String.format("%s.csv", flowcell.getName()))).findAny().orElse(null);
                if (flowcellFileData != null) {
                    sampleSheetFile = new File(flowcellFileData.getPath(), flowcellFileData.getName());
                }

                if (sampleSheetFile == null) {
                    logger.error("SampleSheet is null: {}", flowcell.toString());
                    throw new WorkflowException("SampleSheet is null: " + flowcell.getName());
                }

                if (!sampleSheetFile.exists()) {
                    logger.error("Specified sample sheet doesn't exist: {}", sampleSheetFile.getAbsolutePath());
                    throw new WorkflowException("Invalid SampleSheet: " + flowcell.getName());
                }

                Attribute readCountAttribute = flowcell.getAttributes().stream().filter(a -> "readCount".equals(a.getName())).findAny()
                        .orElse(null);
                Integer readCount = null;
                if (readCountAttribute != null) {
                    readCount = Integer.valueOf(readCountAttribute.getValue());
                }

                File bclDir = new File(flowcell.getBaseDirectory());
                File bclFlowcellDir = new File(bclDir, flowcell.getName());
                File dataDir = new File(bclFlowcellDir, "Data");
                File intensitiesDir = new File(dataDir, "Intensities");
                File baseCallsDir = new File(intensitiesDir, "BaseCalls");

                File flowcellStagingDir = new File(flowcellStagingDirectory, flowcell.getName());

                List<Sample> sampleList = getWorkflowBeanService().getMaPSeqDAOBeanService().getSampleDAO()
                        .findByFlowcellId(flowcell.getId());

                Map<Integer, List<Sample>> laneMap = new HashMap<Integer, List<Sample>>();

                if (CollectionUtils.isNotEmpty(sampleList)) {
                    logger.info("sampleList.size() = {}", sampleList.size());
                    for (Sample sample : sampleList) {
                        if ("Undetermined".equals(sample.getBarcode())) {
                            continue;
                        }
                        laneMap.putIfAbsent(sample.getLaneIndex(), new ArrayList<Sample>());
                        laneMap.get(sample.getLaneIndex()).add(sample);
                    }
                }

                CondorJob copyFromStagingJob = null;

                CondorJob removeExistingBCLDirectoryJob = null;

                if (flowcellStagingDir.exists()) {

                    if (bclFlowcellDir.exists()) {
                        CondorJobBuilder builder = WorkflowJobFactory.createJob(++count, RemoveCLI.class, attempt.getId())
                                .siteName(siteName);
                        builder.addArgument(RemoveCLI.FILE, bclFlowcellDir.getAbsolutePath());
                        removeExistingBCLDirectoryJob = builder.build();
                        logger.info(removeExistingBCLDirectoryJob.toString());
                        graph.addVertex(removeExistingBCLDirectoryJob);
                    }

                    CondorJobBuilder builder = WorkflowJobFactory.createJob(++count, CopyDirectoryCLI.class, attempt.getId())
                            .siteName(siteName);
                    builder.addArgument(CopyDirectoryCLI.SOURCE, flowcellStagingDir.getAbsolutePath())
                            .addArgument(CopyDirectoryCLI.DESTINATION, bclFlowcellDir.getAbsolutePath());
                    copyFromStagingJob = builder.build();
                    logger.info(copyFromStagingJob.toString());
                    graph.addVertex(copyFromStagingJob);
                    if (removeExistingBCLDirectoryJob != null) {
                        graph.addEdge(removeExistingBCLDirectoryJob, copyFromStagingJob);
                    }
                }

                if (MapUtils.isNotEmpty(laneMap)) {

                    // File runInfoXmlFile = new File(flowcellStagingDir, "RunInfo.xml");
                    // String basesMask = Executors.newSingleThreadExecutor()
                    // .submit(new CreateBasesMaskCallable(runInfoXmlFile, sampleSheetFile)).get();

                    for (Integer laneIndex : laneMap.keySet()) {

                        File unalignedDir = new File(bclFlowcellDir, String.format("%s.%d", "Unaligned", laneIndex));

                        CondorJobBuilder builder = WorkflowJobFactory.createJob(++count, BCL2FastqCLI.class, attempt.getId())
                                .numberOfProcessors(5).siteName(siteName);
                        builder.addArgument(BCL2FastqCLI.INPUTDIR, baseCallsDir.getAbsolutePath())
                                .addArgument(BCL2FastqCLI.IGNOREMISSINGBCLS)// .addArgument(BCL2FastqCLI.USESBASESMASK,
                                                                            // basesMask)
                                .addArgument(BCL2FastqCLI.TILES, String.format("s_%d_*", laneIndex))
                                .addArgument(BCL2FastqCLI.OUTPUTDIR, unalignedDir.getAbsolutePath())
                                .addArgument(BCL2FastqCLI.LOADINGTHREADS, "4").addArgument(BCL2FastqCLI.PROCESSINGTHREADS, "4")
                                .addArgument(BCL2FastqCLI.WRITINGTHREADS, "4")
                                .addArgument(BCL2FastqCLI.RUNFOLDERDIR, bclFlowcellDir.getAbsolutePath())
                                .addArgument(BCL2FastqCLI.SAMPLESHEET, sampleSheetFile.getAbsolutePath());

                        if (allowMismatches) {
                            builder.addArgument(BCL2FastqCLI.BARCODEMISMATCHES, 0);
                        }

                        CondorJob bcl2FastqJob = builder.build();
                        logger.info(bcl2FastqJob.toString());
                        graph.addVertex(bcl2FastqJob);

                        if (copyFromStagingJob != null) {
                            graph.addEdge(copyFromStagingJob, bcl2FastqJob);
                        }

                        List<CondorJob> copyJobList = new ArrayList<CondorJob>();

                        CondorJob copyRead1Job = null;
                        CondorJob copyRead2Job = null;

                        for (Sample sample : laneMap.get(laneIndex)) {

                            File workflowDirectory = SequencingWorkflowUtil.createOutputDirectory(sample,
                                    getWorkflowRunAttempt().getWorkflowRun().getWorkflow());

                            File tmpDirectory = new File(workflowDirectory, "tmp");
                            tmpDirectory.mkdirs();

                            logger.info("workflowDirectory.getAbsolutePath(): {}", workflowDirectory.getAbsolutePath());

                            File projectDirectory = new File(unalignedDir, sample.getStudy().getName());

                            File outputFile = null;

                            switch (readCount) {
                                case 1:
                                    builder = SequencingWorkflowJobFactory
                                            .createJob(++count, CopyFile2CLI.class, attempt.getId(), sample.getId()).siteName(siteName);

                                    outputFile = new File(workflowDirectory, String.format("%s_%s_L%03d_R%d.fastq.gz", flowcell.getName(),
                                            sample.getBarcode(), laneIndex, 1));

                                    builder.addArgument(CopyFile2CLI.SOURCEPARENTDIR, projectDirectory.getAbsolutePath())
                                            .addArgument(CopyFile2CLI.SOURCEPREFIX, sample.getName())
                                            .addArgument(CopyFile2CLI.SOURCESUFFIX, String.format("L%03d_R1_001.fastq.gz", laneIndex))
                                            .addArgument(CopyFile2CLI.DESTINATION, outputFile.getAbsolutePath())
                                            .addArgument(CopyFile2CLI.MIMETYPE, MimeType.FASTQ.toString());
                                    copyRead1Job = builder.build();
                                    logger.info(copyRead1Job.toString());
                                    graph.addVertex(copyRead1Job);
                                    graph.addEdge(bcl2FastqJob, copyRead1Job);
                                    copyJobList.add(copyRead1Job);
                                    break;
                                case 2:
                                default:

                                    // read 1
                                    builder = SequencingWorkflowJobFactory
                                            .createJob(++count, CopyFile2CLI.class, attempt.getId(), sample.getId()).siteName(siteName);

                                    outputFile = new File(workflowDirectory,
                                            String.format("%s_%s_L%03d_R1.fastq.gz", flowcell.getName(), sample.getBarcode(), laneIndex));

                                    builder.addArgument(CopyFile2CLI.SOURCEPARENTDIR, projectDirectory.getAbsolutePath())
                                            .addArgument(CopyFile2CLI.SOURCEPREFIX, sample.getName())
                                            .addArgument(CopyFile2CLI.SOURCESUFFIX, String.format("L%03d_R1_001.fastq.gz", laneIndex))
                                            .addArgument(CopyFile2CLI.DESTINATION, outputFile.getAbsolutePath())
                                            .addArgument(CopyFile2CLI.MIMETYPE, MimeType.FASTQ.toString());
                                    copyRead1Job = builder.build();
                                    logger.info(copyRead1Job.toString());
                                    graph.addVertex(copyRead1Job);
                                    graph.addEdge(bcl2FastqJob, copyRead1Job);
                                    copyJobList.add(copyRead1Job);

                                    // read 2
                                    builder = SequencingWorkflowJobFactory
                                            .createJob(++count, CopyFile2CLI.class, attempt.getId(), sample.getId()).siteName(siteName);

                                    outputFile = new File(workflowDirectory,
                                            String.format("%s_%s_L%03d_R2.fastq.gz", flowcell.getName(), sample.getBarcode(), laneIndex));

                                    builder.addArgument(CopyFile2CLI.SOURCEPARENTDIR, projectDirectory.getAbsolutePath())
                                            .addArgument(CopyFile2CLI.SOURCEPREFIX, sample.getName())
                                            .addArgument(CopyFile2CLI.SOURCESUFFIX, String.format("L%03d_R2_001.fastq.gz", laneIndex))
                                            .addArgument(CopyFile2CLI.DESTINATION, outputFile.getAbsolutePath())
                                            .addArgument(CopyFile2CLI.MIMETYPE, MimeType.FASTQ.toString());
                                    copyRead2Job = builder.build();
                                    logger.info(copyRead2Job.toString());
                                    graph.addVertex(copyRead2Job);
                                    graph.addEdge(bcl2FastqJob, copyRead2Job);
                                    copyJobList.add(copyRead2Job);
                                    break;
                            }

                        }

                    }

                }

            }
        } catch (Exception e) {
            throw new WorkflowException(e);
        }

        return graph;
    }

    @Override
    public void init() throws WorkflowException {
        super.init();

        try {
            List<Flowcell> flowcellList = getWorkflowBeanService().getMaPSeqDAOBeanService().getFlowcellDAO()
                    .findByWorkflowRunId(getWorkflowRunAttempt().getWorkflowRun().getId());

            if (CollectionUtils.isNotEmpty(flowcellList)) {
                String flowcellStagingDirectory = getWorkflowBeanService().getAttributes().get("flowcellStagingDirectory");

                for (Flowcell flowcell : flowcellList) {
                    File flowcellStagingDir = new File(flowcellStagingDirectory, flowcell.getName());

                    File runInfoXmlFile = new File(flowcellStagingDir, "RunInfo.xml");
                    if (!runInfoXmlFile.exists()) {
                        logger.error("RunInfo.xml file does not exist: {}", runInfoXmlFile.getAbsolutePath());
                        throw new WorkflowException("Invalid SampleSheet: " + flowcell.getName());
                    }

                    Integer readCount = Executors.newSingleThreadExecutor().submit(new FindReadCountCallable(runInfoXmlFile)).get();

                    logger.debug("readCount = {}", readCount);

                    Set<String> attributeNameSet = new HashSet<String>();
                    if (CollectionUtils.isNotEmpty(flowcell.getAttributes())) {
                        for (Attribute attribute : flowcell.getAttributes()) {
                            attributeNameSet.add(attribute.getName());
                        }
                    }

                    Collections.synchronizedSet(attributeNameSet);
                    if (attributeNameSet.contains("readCount")) {
                        for (Attribute attribute : flowcell.getAttributes()) {
                            if ("readCount".equals(attribute.getName())) {
                                attribute.setValue(readCount.toString());
                                getWorkflowBeanService().getMaPSeqDAOBeanService().getAttributeDAO().save(attribute);
                                break;
                            }
                        }
                    } else {
                        Attribute attribute = new Attribute("readCount", readCount.toString());
                        attribute.setId(getWorkflowBeanService().getMaPSeqDAOBeanService().getAttributeDAO().save(attribute));
                        flowcell.getAttributes().add(attribute);
                        getWorkflowBeanService().getMaPSeqDAOBeanService().getFlowcellDAO().save(flowcell);
                    }

                }

            }
        } catch (MaPSeqDAOException | InterruptedException | ExecutionException e) {
            logger.error("init error", e);
            throw new WorkflowException("init error: ", e);
        }

    }

    @Override
    public void postRun() throws WorkflowException {

        WorkflowRunAttempt attempt = getWorkflowRunAttempt();

        MaPSeqDAOBeanService mapseqDAOBeanService = getWorkflowBeanService().getMaPSeqDAOBeanService();

        ExecutorService es = Executors.newSingleThreadExecutor();

        try {
            List<Flowcell> flowcellList = mapseqDAOBeanService.getFlowcellDAO().findByWorkflowRunId(attempt.getWorkflowRun().getId());

            if (CollectionUtils.isNotEmpty(flowcellList)) {

                for (Flowcell flowcell : flowcellList) {

                    RegisterToIRODSRunnable registerToIRODSRunnable = new RegisterToIRODSRunnable(mapseqDAOBeanService);
                    registerToIRODSRunnable.setFlowcellId(flowcell.getId());
                    es.submit(registerToIRODSRunnable);

                    SaveDemultiplexedStatsAttributesRunnable saveDemultiplexedStatsAttributesRunnable = new SaveDemultiplexedStatsAttributesRunnable(
                            mapseqDAOBeanService, flowcell);
                    es.submit(saveDemultiplexedStatsAttributesRunnable);

                    // SaveObservedClusterDensityAttributesRunnable saveObservedClusterDensityAttributesRunnable = new
                    // SaveObservedClusterDensityAttributesRunnable(
                    // mapseqDAOBeanService, flowcell);
                    // es.submit(saveObservedClusterDensityAttributesRunnable);

                }

            }

            es.shutdown();
            es.awaitTermination(1L, TimeUnit.HOURS);
        } catch (InterruptedException | MaPSeqDAOException e) {
            e.printStackTrace();
        }

    }

}
