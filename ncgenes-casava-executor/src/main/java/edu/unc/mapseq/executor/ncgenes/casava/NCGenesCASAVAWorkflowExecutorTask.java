package edu.unc.mapseq.executor.ncgenes.casava;

import java.util.Date;
import java.util.List;
import java.util.TimerTask;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.dao.MaPSeqDAOBeanService;
import edu.unc.mapseq.dao.MaPSeqDAOException;
import edu.unc.mapseq.dao.WorkflowDAO;
import edu.unc.mapseq.dao.WorkflowRunAttemptDAO;
import edu.unc.mapseq.dao.model.Workflow;
import edu.unc.mapseq.dao.model.WorkflowRunAttempt;
import edu.unc.mapseq.workflow.WorkflowBeanService;
import edu.unc.mapseq.workflow.WorkflowExecutor;
import edu.unc.mapseq.workflow.WorkflowTPE;
import edu.unc.mapseq.workflow.ncgenes.casava.NCGenesCASAVAWorkflow;

public class NCGenesCASAVAWorkflowExecutorTask extends TimerTask {

    private static final Logger logger = LoggerFactory.getLogger(NCGenesCASAVAWorkflowExecutorTask.class);

    private final WorkflowTPE threadPoolExecutor = new WorkflowTPE();

    private String workflowName;

    private WorkflowBeanService workflowBeanService;

    public NCGenesCASAVAWorkflowExecutorTask() {
        super();
    }

    @Override
    public void run() {
        logger.info("ENTERING run()");

        threadPoolExecutor.setCorePoolSize(workflowBeanService.getCorePoolSize());
        threadPoolExecutor.setMaximumPoolSize(workflowBeanService.getMaxPoolSize());

        logger.info(String.format("ActiveCount: %d, TaskCount: %d, CompletedTaskCount: %d", threadPoolExecutor.getActiveCount(),
                threadPoolExecutor.getTaskCount(), threadPoolExecutor.getCompletedTaskCount()));

        MaPSeqDAOBeanService maPSeqDAOBeanService = this.workflowBeanService.getMaPSeqDAOBeanService();

        WorkflowDAO workflowDAO = maPSeqDAOBeanService.getWorkflowDAO();
        WorkflowRunAttemptDAO workflowRunAttemptDAO = maPSeqDAOBeanService.getWorkflowRunAttemptDAO();

        try {
            List<Workflow> workflowList = workflowDAO.findByName(getWorkflowName());

            if (CollectionUtils.isEmpty(workflowList)) {
                logger.error("No Workflow Found: {}", getWorkflowName());
                return;
            }

            Workflow workflow = workflowList.get(0);

            List<WorkflowRunAttempt> attempts = workflowRunAttemptDAO.findEnqueued(workflow.getId());

            if (CollectionUtils.isNotEmpty(attempts)) {

                logger.info("dequeuing {} WorkflowRunAttempt", attempts.size());
                for (WorkflowRunAttempt attempt : attempts) {

                    NCGenesCASAVAWorkflow casavaWorkflow = new NCGenesCASAVAWorkflow();
                    attempt.setVersion(casavaWorkflow.getVersion());
                    attempt.setDequeued(new Date());
                    workflowRunAttemptDAO.save(attempt);

                    casavaWorkflow.setWorkflowBeanService(workflowBeanService);
                    casavaWorkflow.setWorkflowRunAttempt(attempt);
                    threadPoolExecutor.submit(new WorkflowExecutor(casavaWorkflow));

                }

            }

        } catch (MaPSeqDAOException e) {
            e.printStackTrace();
        }

    }

    public WorkflowBeanService getWorkflowBeanService() {
        return workflowBeanService;
    }

    public void setWorkflowBeanService(WorkflowBeanService workflowBeanService) {
        this.workflowBeanService = workflowBeanService;
    }

    public String getWorkflowName() {
        return workflowName;
    }

    public void setWorkflowName(String workflowName) {
        this.workflowName = workflowName;
    }

}
