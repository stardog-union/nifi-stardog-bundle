package com.stardog.nifi;

import java.io.File;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.complexible.common.io.Files2;
import com.complexible.stardog.api.Connection;
import com.complexible.stardog.virtual.api.admin.VirtualGraphAdminConnection;
import com.complexible.stardog.virtual.api.admin.VirtualGraphAdminConnection.InputFileType;
import com.stardog.stark.IRI;
import com.stardog.stark.Values;
import com.stardog.stark.io.FileFormat;
import com.stardog.stark.io.RDFFormat;
import com.stardog.stark.io.RDFFormats;
import com.stardog.stark.query.io.QueryResultFormat;
import com.stardog.stark.query.io.QueryResultFormats;

import com.google.common.base.Charsets;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

@Tags({"stardog", "put", "write", "rdf", "csv", "json"})
@CapabilityDescription("Put data into a Stardog database. Data in RDF format is added directly whereas CSV and JSON input " +
                       "are imported into Stardog via the provided mapping file.")
@EventDriven
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@SeeAlso({})
public class StardogPut extends AbstractStardogProcessor {
    // Impl note: We are cheating here by using QueryResultFormats constants for CSV and JSON input
    private static final Map<String, FileFormat> INPUT_FORMATS =
            ImmutableMap.<String,  FileFormat>builder()
                    .put("CSV", QueryResultFormats.CSV)
                    .put("JSON", QueryResultFormats.JSON)
                    .put("JSON-LD", RDFFormats.JSONLD)
                    .put("RDF/XML", RDFFormats.RDFXML)
                    .put("Turtle", RDFFormats.TURTLE)
                    .put("TriG", RDFFormats.TRIG)
                    .put("N-Triples", RDFFormats.NTRIPLES)
                    .put("N-Quads", RDFFormats.NQUADS)
                    .build();

    public static final PropertyDescriptor MAPPING_FILE =
            new PropertyDescriptor.Builder()
                    .name("Mappings File")
                    .description("The mapping file to be used when loading CSV or JSON data into Stardog. The mapping should be" +
                                 "provided in Stardog Mapping Syntax (SMS). The mapping file is not required " +
                                 "if input is already in RDF format.")
                    .required(false)
                    .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                    .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
                    .build();

    // TODO allow R2RML syntax
//    public static final PropertyDescriptor MAPPING_SYNTAX =
//            new PropertyDescriptor.Builder()
//                    .name("Mappings Syntax")
//                    .description("The syntax used for the mapping file.")
//                    .required(false)
//                    .defaultValue(VirtualGraphMappingSyntax.MAPPING_SYNTAX_DEFAULT)
//                    .allowableValues(VirtualGraphMappingSyntax.MAPPING_SYNTAX_DEFAULT,
//                                     VirtualGraphMappingSyntax.SMS2.name(),
//                                     VirtualGraphMappingSyntax.R2RML.name())
//                    .build();

    public static final PropertyDescriptor INPUT_FORMAT =
            new PropertyDescriptor.Builder()
                    .name("Input Format")
                    .description("The format of the input data. CSV and JSON formats require input data to be mapped to RDF " +
                                 "with a provided mappings file. Other formats are RDF formats that can be loaded directly " +
                                 "without mappings. Note that JSON-LD is a special kind of JSON format for representing RDF " +
                                 "data. If this parameter is not specified the input format will be automatically determined " +
                                 "from the input file mime type.")
                    .required(false)
                    .allowableValues(INPUT_FORMATS.keySet())
                    .build();


    public static final PropertyDescriptor TARGET_GRAPH =
            new PropertyDescriptor.Builder()
                    .name("Target Graph")
                    .description("The destination named graph where the data will be loaded into. Data will be loaded into the " +
                                 "DEFAULT graph by default.")
                    .required(false)
                    .addValidator(IRI_VALIDATOR)
                    .build();

    public static final PropertyDescriptor CLEAR_TARGET_GRAPH =
            new PropertyDescriptor.Builder()
                    .name("Clear Target Graph")
                    .description("Clear the target graph before putting the data. Clear operation will be done in the same " +
                                 "transaction that inserts the data for RDF inputs. If the input is being mapped from CSV or " +
                                 "JSON then clear operation will be a separate transaction.")
                    .required(true)
                    .defaultValue("false")
                    .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                    .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
                    .build();

    private static final List<PropertyDescriptor> PROPERTIES =
            ImmutableList.<PropertyDescriptor>builder()
                    .addAll(DEFAULT_PROPERTIES)
                    .add(INPUT_FORMAT)
                    .add(MAPPING_FILE)
                    .add(TARGET_GRAPH)
                    .add(CLEAR_TARGET_GRAPH)
                    .build();

    @Override
    protected void init(ProcessorInitializationContext context) {

    }

    @Override
    public Set<Relationship> getRelationships() {
        return DEFAULT_RELATIONSHIPS;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile inputFile = session.get();
        if (inputFile == null) {
            return;
        }

        Stopwatch stopwatch = Stopwatch.createStarted();

        ComponentLog logger = getLogger();

        try (Connection connection = connect(context);
             InputStream in = session.read(inputFile)) {

            IRI targetGraph =  toIRI(context.getProperty(TARGET_GRAPH).evaluateAttributeExpressions(inputFile).getValue(), connection, Values.DEFAULT_GRAPH);
            boolean clearTargetGraph =  context.getProperty(CLEAR_TARGET_GRAPH).evaluateAttributeExpressions(inputFile).asBoolean();

            String selectedFormat = context.getProperty(INPUT_FORMAT).getValue();
            FileFormat inputFormat =  INPUT_FORMATS.get(selectedFormat);

            logger.info("Input format for ingestion {} ({})", new Object[] { inputFormat, inputFormat.getClass().getSimpleName() });

            if (inputFormat instanceof QueryResultFormat) {
                VirtualGraphAdminConnection vgConn = connection.admin().as(VirtualGraphAdminConnection.class);
                File mappingFile = new File(context.getProperty(MAPPING_FILE).evaluateAttributeExpressions(inputFile).getValue());
                String mappingString = Files2.toString(mappingFile.toPath(), Charsets.UTF_8);

                if (clearTargetGraph) {
                    connection.begin();
                    connection.remove().context(targetGraph);
                    connection.commit();
                }

                InputFileType fileType = inputFormat.equals(QueryResultFormats.JSON)
                                         ? InputFileType.JSON
                                         : InputFileType.DELIMITED;
                // TODO make properties customizable
                vgConn.importFile(mappingString, new Properties(), connection.name(), targetGraph, in, fileType);
            }
            else {
                connection.begin();
                if (clearTargetGraph) {
                    connection.remove().context(targetGraph);
                }
                connection.add()
                          .io()
                          .format((RDFFormat) inputFormat)
                          .context(targetGraph)
                          .stream(in);
                connection.commit();
            }

            logger.info("Finished ingesting data into Stardog; transferring to 'success'", new Object[] { });
            session.getProvenanceReporter()
                   .modifyContent(inputFile, "Ingested data into Stardog", stopwatch.elapsed(TimeUnit.MILLISECONDS));
            session.transfer(inputFile, REL_SUCCESS);
        }
        catch (Throwable t) {
            Throwable rootCause = Throwables.getRootCause(t);
            context.yield();
            logger.error("{} failed! Throwable exception {}; rolling back session", new Object[] { this, rootCause });
            session.transfer(inputFile, REL_FAILURE);
        }
    }
}
