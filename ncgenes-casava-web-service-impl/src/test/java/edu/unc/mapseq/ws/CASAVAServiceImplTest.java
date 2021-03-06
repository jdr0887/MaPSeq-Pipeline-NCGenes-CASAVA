package edu.unc.mapseq.ws;

import java.io.File;
import java.net.MalformedURLException;

import javax.activation.DataHandler;
import javax.xml.namespace.QName;
import javax.xml.ws.Binding;
import javax.xml.ws.BindingProvider;
import javax.xml.ws.Service;
import javax.xml.ws.soap.SOAPBinding;

import org.apache.cxf.endpoint.Client;
import org.apache.cxf.frontend.ClientProxy;
import org.apache.cxf.transport.http.HTTPConduit;
import org.junit.Test;

import edu.unc.mapseq.ws.ncgenes.casava.NCGenesCASAVAService;

public class CASAVAServiceImplTest {

    @Test
    public void testUpload() {
        
        QName serviceQName = new QName("http://casava.ncgenes.ws.mapseq.unc.edu", "NCGenesCASAVAService");
        Service service = Service.create(serviceQName);
        QName portQName = new QName("http://casava.ncgenes.ws.mapseq.unc.edu", "NCGenesCASAVAPort");
        service.addPort(portQName, SOAPBinding.SOAP11HTTP_BINDING, String.format("http://%s:%d/cxf/NCGenesCASAVAService", "152.54.3.109", 8181));
        NCGenesCASAVAService casavaService = service.getPort(NCGenesCASAVAService.class);

        Client cl = ClientProxy.getClient(casavaService);
        HTTPConduit httpConduit = (HTTPConduit) cl.getConduit();
        httpConduit.getClient().setReceiveTimeout(5 * 60 * 1000L);

        Binding binding = ((BindingProvider) service.getPort(portQName, NCGenesCASAVAService.class)).getBinding();
        ((SOAPBinding) binding).setMTOMEnabled(true);

        try {
            // File f = new File("/home/jdr0887", "140912_UNC17-D00216_0247_BC4G46ANXX.csv");
            //File f = new File("/home/jdr0887", "141006_UNC17-D00216_0249_BC4G45ANXX.csv");
            File f = new File("/home/jdr0887", "161003_UNC17-D00216_0410_BC9T7RANXX.csv");
            DataHandler handler = new DataHandler(f.toURI().toURL());
            Long id = casavaService.uploadSampleSheet(handler, "161003_UNC17-D00216_0410_BC9T7RANXX");
            System.out.println(id);
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testAssertDirectoryExists() {
        QName serviceQName = new QName("http://ws.mapseq.unc.edu", "SequencerRunService");
        QName portQName = new QName("http://ws.mapseq.unc.edu", "SequencerRunPort");
        Service service = Service.create(serviceQName);
        service.addPort(portQName, SOAPBinding.SOAP11HTTP_MTOM_BINDING,
                String.format("http://%s:%d/cxf/SequencerRunService", "biodev2.its.unc.edu", 8181));
        NCGenesCASAVAService casavaService = service.getPort(NCGenesCASAVAService.class);
        System.out.println(casavaService.assertDirectoryExists("UC", "121212_UNC13-SN749_0207_BD1LCDACXX"));
        System.out.println(casavaService.assertDirectoryExists("UC", "130116_UNC16-SN851_0202_AD1TP9ACXX"));
        System.out.println(casavaService.assertDirectoryExists("UC", "130117_UNC14-SN744_0297_BD1RDUACXX"));
        System.out.println(casavaService.assertDirectoryExists("UC", "130129_UNC15-SN850_0257_BC1M1EACXX"));
        System.out.println(casavaService.assertDirectoryExists("UC", "130201_UNC15-SN850_0258_AD1RW7ACXX"));
        System.out.println(casavaService.assertDirectoryExists("UC", "130201_UNC16-SN851_0210_BC1NN7ACXX"));
        System.out.println(casavaService.assertDirectoryExists("UC", "130220_UNC13-SN749_0232_AD1VEJACXX"));
        System.out.println(casavaService.assertDirectoryExists("UC", "130118_UNC16-SN851_0205_BC1HHAACXX"));

    }

}
