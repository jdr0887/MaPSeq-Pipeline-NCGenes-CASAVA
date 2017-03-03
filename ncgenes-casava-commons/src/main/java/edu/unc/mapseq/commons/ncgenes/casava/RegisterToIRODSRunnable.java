package edu.unc.mapseq.commons.ncgenes.casava;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.FrameworkUtil;
import org.renci.common.exec.BashExecutor;
import org.renci.common.exec.CommandInput;
import org.renci.common.exec.CommandOutput;
import org.renci.common.exec.Executor;
import org.renci.common.exec.ExecutorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.dao.MaPSeqDAOBeanService;
import edu.unc.mapseq.dao.MaPSeqDAOException;
import edu.unc.mapseq.dao.model.MimeType;
import edu.unc.mapseq.dao.model.Sample;
import edu.unc.mapseq.dao.model.Workflow;
import edu.unc.mapseq.workflow.sequencing.IRODSBean;
import edu.unc.mapseq.workflow.sequencing.SequencingWorkflowUtil;

public class RegisterToIRODSRunnable implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(RegisterToIRODSRunnable.class);

    private MaPSeqDAOBeanService maPSeqDAOBeanService;

    private Long flowcellId;

    private Long sampleId;

    public RegisterToIRODSRunnable(MaPSeqDAOBeanService maPSeqDAOBeanService) {
        super();
        this.maPSeqDAOBeanService = maPSeqDAOBeanService;
    }

    @Override
    public void run() {
        logger.info("ENTERING run()");

        Set<Sample> sampleSet = new HashSet<Sample>();

        List<Workflow> workflowList = null;

        try {
            if (sampleId != null) {
                sampleSet.add(maPSeqDAOBeanService.getSampleDAO().findById(sampleId));
            }

            if (flowcellId != null) {
                List<Sample> samples = maPSeqDAOBeanService.getSampleDAO().findByFlowcellId(flowcellId);
                if (samples != null && !samples.isEmpty()) {
                    sampleSet.addAll(samples);
                }
            }

            workflowList = maPSeqDAOBeanService.getWorkflowDAO().findByName("NCGenesCASAVA");
            if (CollectionUtils.isEmpty(workflowList)) {
                return;
            }

        } catch (MaPSeqDAOException e1) {
            e1.printStackTrace();
            return;
        }

        Workflow workflow = workflowList.get(0);

        BundleContext bundleContext = FrameworkUtil.getBundle(getClass()).getBundleContext();
        Bundle bundle = bundleContext.getBundle();
        String version = bundle.getVersion().toString();

        ExecutorService es = Executors.newSingleThreadExecutor();
        for (Sample sample : sampleSet) {
            es.submit(() -> {

                File outputDirectory = SequencingWorkflowUtil.createOutputDirectory(sample, workflow);
                File tmpDir = new File(outputDirectory, "tmp");
                if (!tmpDir.exists()) {
                    tmpDir.mkdirs();
                }

                List<File> readPairList = SequencingWorkflowUtil.getReadPairList(sample);

                // assumption: a dash is used as a delimiter between a participantId and the external code
                // int idx = sample.getName().lastIndexOf("-");
                // String participantId = idx != -1 ? sample.getName().substring(0, idx) : sample.getName();
                // no longer expecting composite for participantId...can just use sample name
                String participantId = sample.getName();

                String irodsDirectory = String.format("/MedGenZone/%s/sequencing/ncgenes/analysis/%s/L%03d_%s/%s",
                        workflow.getSystem().getValue(), sample.getFlowcell().getName(), sample.getLaneIndex(), sample.getBarcode(),
                        workflow.getName());

                CommandOutput commandOutput = null;

                List<CommandInput> commandInputList = new LinkedList<CommandInput>();

                CommandInput commandInput = new CommandInput();
                commandInput.setExitImmediately(Boolean.FALSE);
                StringBuilder sb = new StringBuilder();
                sb.append(String.format("$IRODS_HOME/imkdir -p %s%n", irodsDirectory));
                sb.append(String.format("$IRODS_HOME/imeta add -C %s Project NCGENES%n", irodsDirectory));
                sb.append(String.format("$IRODS_HOME/imeta add -C %s ParticipantID %s NCGENES%n", irodsDirectory, participantId));
                commandInput.setCommand(sb.toString());
                commandInput.setWorkDir(tmpDir);
                commandInputList.add(commandInput);

                List<IRODSBean> files2RegisterToIRODS = new ArrayList<IRODSBean>();

                List<ImmutablePair<String, String>> attributeList = Arrays.asList(
                        new ImmutablePair<String, String>("ParticipantId", participantId),
                        new ImmutablePair<String, String>("MaPSeqWorkflowVersion", version),
                        new ImmutablePair<String, String>("MaPSeqWorkflowName", workflow.getName()),
                        new ImmutablePair<String, String>("MaPSeqMimeType", MimeType.FASTQ.toString()),
                        new ImmutablePair<String, String>("MaPSeqStudyName", sample.getStudy().getName()),
                        new ImmutablePair<String, String>("MaPSeqSampleId", sample.getId().toString()),
                        new ImmutablePair<String, String>("MaPSeqSystem", workflow.getSystem().getValue()),
                        new ImmutablePair<String, String>("MaPSeqFlowcellId", sample.getFlowcell().getId().toString()));

                files2RegisterToIRODS.add(new IRODSBean(readPairList.get(0), attributeList));
                files2RegisterToIRODS.add(new IRODSBean(readPairList.get(1), attributeList));

                for (IRODSBean bean : files2RegisterToIRODS) {

                    commandInput = new CommandInput();
                    commandInput.setExitImmediately(Boolean.FALSE);

                    File f = bean.getFile();
                    if (!f.exists()) {
                        logger.warn("file to register doesn't exist: {}", f.getAbsolutePath());
                        continue;
                    }

                    StringBuilder registerCommandSB = new StringBuilder();
                    String registrationCommand = String.format("$IRODS_HOME/ireg -f %s %s/%s", bean.getFile().getAbsolutePath(),
                            irodsDirectory, bean.getFile().getName());
                    String deRegistrationCommand = String.format("$IRODS_HOME/irm -U %s/%s", irodsDirectory, bean.getFile().getName());
                    registerCommandSB.append(registrationCommand).append("\n");
                    registerCommandSB
                            .append(String.format("if [ $? != 0 ]; then %s; %s; fi%n", deRegistrationCommand, registrationCommand));
                    commandInput.setCommand(registerCommandSB.toString());
                    commandInput.setWorkDir(tmpDir);
                    commandInputList.add(commandInput);

                    commandInput = new CommandInput();
                    commandInput.setExitImmediately(Boolean.FALSE);
                    sb = new StringBuilder();
                    for (ImmutablePair<String, String> attribute : bean.getAttributes()) {
                        sb.append(String.format("$IRODS_HOME/imeta add -d %s/%s %s %s NCGenes%n", irodsDirectory, bean.getFile().getName(),
                                attribute.getLeft(), attribute.getRight()));
                    }
                    commandInput.setCommand(sb.toString());
                    commandInput.setWorkDir(tmpDir);
                    commandInputList.add(commandInput);

                }

                File mapseqrc = new File(System.getProperty("user.home"), ".mapseqrc");
                Executor executor = BashExecutor.getInstance();

                for (CommandInput ci : commandInputList) {
                    try {
                        logger.debug("ci.getCommand(): {}", ci.getCommand());
                        commandOutput = executor.execute(ci, mapseqrc);
                        if (commandOutput.getExitCode() != 0) {
                            logger.info("commandOutput.getExitCode(): {}", commandOutput.getExitCode());
                            logger.warn("command failed: {}", ci.getCommand());
                        }
                        logger.debug("commandOutput.getStdout(): {}", commandOutput.getStdout());
                    } catch (ExecutorException e) {
                        if (commandOutput != null) {
                            logger.warn("commandOutput.getStderr(): {}", commandOutput.getStderr());
                        }
                    }
                }

                logger.info("FINISHED PROCESSING: {}", sample.toString());

            });

        }

    }

    public MaPSeqDAOBeanService getMaPSeqDAOBeanService() {
        return maPSeqDAOBeanService;
    }

    public void setMaPSeqDAOBeanService(MaPSeqDAOBeanService maPSeqDAOBeanService) {
        this.maPSeqDAOBeanService = maPSeqDAOBeanService;
    }

    public Long getSampleId() {
        return sampleId;
    }

    public void setSampleId(Long sampleId) {
        this.sampleId = sampleId;
    }

    public Long getFlowcellId() {
        return flowcellId;
    }

    public void setFlowcellId(Long flowcellId) {
        this.flowcellId = flowcellId;
    }

}
