package edu.unc.mapseq.commands.ncgenes.casava;

import java.util.List;
import java.util.concurrent.Executors;

import org.apache.karaf.shell.api.action.Action;
import org.apache.karaf.shell.api.action.Argument;
import org.apache.karaf.shell.api.action.Command;
import org.apache.karaf.shell.api.action.lifecycle.Reference;
import org.apache.karaf.shell.api.action.lifecycle.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.commons.ncgenes.casava.SaveObservedClusterDensityAttributesRunnable;
import edu.unc.mapseq.config.MaPSeqConfigurationService;
import edu.unc.mapseq.dao.MaPSeqDAOBeanService;

@Command(scope = "ncgenes-casava", name = "save-observed-cluster-density-attributes", description = "Save Observed Cluster Density Attributes")
@Service
public class SaveObservedClusterDensityAttributesAction implements Action {

    private final Logger logger = LoggerFactory.getLogger(SaveObservedClusterDensityAttributesAction.class);

    @Reference
    private MaPSeqDAOBeanService maPSeqDAOBeanService;

    @Reference
    private MaPSeqConfigurationService maPSeqConfigurationService;

    @Argument(index = 0, name = "flowcellId", required = true, multiValued = true)
    private List<Long> flowcellIdList;

    @Override
    public Object execute() throws Exception {
        logger.debug("ENTERING execute()");
        SaveObservedClusterDensityAttributesRunnable runnable = new SaveObservedClusterDensityAttributesRunnable();
        runnable.setMaPSeqDAOBeanService(maPSeqDAOBeanService);
        runnable.setMaPSeqConfigurationService(maPSeqConfigurationService);
        runnable.setFlowcellIdList(flowcellIdList);
        Executors.newSingleThreadExecutor().execute(runnable);
        return null;
    }

    public List<Long> getFlowcellIdList() {
        return flowcellIdList;
    }

    public void setFlowcellIdList(List<Long> flowcellIdList) {
        this.flowcellIdList = flowcellIdList;
    }

}
