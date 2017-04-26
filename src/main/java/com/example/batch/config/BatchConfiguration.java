package com.example.batch.config;

import com.example.batch.listener.JobCompletionNotificationListener;
import com.example.batch.listener.StepCompletionNotificationListener;
import com.example.batch.model.Person;
import com.example.batch.processor.PersonItemProcessor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.file.FlatFileHeaderCallback;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;

import java.io.IOException;
import java.io.Writer;

/**
 * Created by mradul on 25/04/17.
 */
@Configuration
@EnableBatchProcessing
@ComponentScan({"com.example.batch.listener"})
public class BatchConfiguration {

    public static String FILE_PATH = "/Users/mradul/IdeaProjects/batch/student-marksheet.csv";

    @Autowired
    public JobBuilderFactory jobBuilderFactory;


    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Autowired
    private StepCompletionNotificationListener stepCompletionNotificationListener;

    @Autowired
    private JobCompletionNotificationListener jobCompletionNotificationListener;


    @Bean
    public FlatFileItemReader<Person> reader() {
        FlatFileItemReader<Person> reader = new FlatFileItemReader<Person>();
        reader.setResource(new ClassPathResource("sample-data.csv"));
        reader.setLineMapper(new DefaultLineMapper<Person>() {{
            setLineTokenizer(new DelimitedLineTokenizer() {{
                setNames(new String[] { "firstName", "lastName" });
            }});
            setFieldSetMapper(new BeanWrapperFieldSetMapper<Person>() {{
                setTargetType(Person.class);
            }});
        }});
        return reader;
    }

    @Bean
    public PersonItemProcessor processor() {
        return new PersonItemProcessor();
    }

    @Bean
    public FlatFileItemWriter<Person> writer() {
        FlatFileItemWriter<Person> writer = new FlatFileItemWriter<Person>();
        writer.setHeaderCallback(new FlatFileHeaderCallback(){
            public void writeHeader(Writer writer) throws IOException {
                writer.write("FIRST_NAME,LAST_NAME,#TOTAL#");
            }
        });
        writer.setResource(new FileSystemResource(FILE_PATH));
        DelimitedLineAggregator<Person> delLineAgg = new DelimitedLineAggregator<Person>();
        delLineAgg.setDelimiter(",");
        BeanWrapperFieldExtractor<Person> fieldExtractor = new BeanWrapperFieldExtractor<Person>();
        fieldExtractor.setNames(new String[] {"firstName", "lastName"});
        delLineAgg.setFieldExtractor(fieldExtractor);
        writer.setLineAggregator(delLineAgg);
        return writer;
    }

    @Bean
    public Job importUserJob() {
        return jobBuilderFactory.get("importUserJob")
                .incrementer(new RunIdIncrementer())
                .listener(jobCompletionNotificationListener)
                .flow(importStep())
                .end()
                .build();
    }

    @Bean
    public Step importStep() {
        return stepBuilderFactory.get("importStep")
                .<Person, Person> chunk(10)
                .reader(reader())
                .processor(processor())
                .writer(writer())
                .listener(stepCompletionNotificationListener)
                .build();
    }


}
