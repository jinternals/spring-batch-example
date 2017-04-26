package com.example.batch.listener;

import com.example.batch.config.BatchConfiguration;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.listener.StepExecutionListenerSupport;
import org.springframework.stereotype.Component;

import java.io.FileInputStream;
import java.io.FileOutputStream;


/**
 * Created by mradul on 25/04/17.
 */
@Component
public class StepCompletionNotificationListener extends StepExecutionListenerSupport {

    private static final Logger log = LoggerFactory.getLogger(StepCompletionNotificationListener.class);

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {

        int total = stepExecution.getWriteCount();
        log.info("Total item count:"+total);

        try {
            String content = IOUtils.toString(new FileInputStream(BatchConfiguration.FILE_PATH), "UTF-8");
            content = content.replaceAll("#TOTAL#", Integer.toString(total));
            IOUtils.write(content, new FileOutputStream(BatchConfiguration.FILE_PATH), "UTF-8");
        }catch (Exception e){
            e.printStackTrace();
        }
       return stepExecution.getExitStatus();
    }

}
