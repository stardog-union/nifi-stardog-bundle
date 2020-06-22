package com.stardog.nifi;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.complexible.common.io.Files2;
import com.complexible.stardog.api.Connection;
import com.complexible.stardog.virtual.api.VirtualGraphOptions;
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
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
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

    public static final Validator CHARACTER_VALIDATOR = new Validator() {
        private final Validator stringLengthValidator = new StandardValidators.StringLengthValidator(1, 1);

        public ValidationResult validate(String subject, String value, ValidationContext context) {
            if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(value)) {
                return (new ValidationResult.Builder()).subject(subject).input(value).explanation("Expression Language Present").valid(true).build();
            } else {
                return stringLengthValidator.validate(subject, value, context);
            }
        }
    };

    public static final PropertyDescriptor MAPPINGS_FILE =
            new PropertyDescriptor.Builder()
                    .name("Mappings File")
                    .description("The mapping file to be used when loading CSV or JSON data into Stardog. The mapping should be " +
                                 "provided in Stardog Mapping Syntax (SMS). The mapping file is not required " +
                                 "if input is already in RDF format.")
                    .required(false)
                    .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                    .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
                    .build();

    public static final PropertyDescriptor PROPERTIES_FILE =
            new PropertyDescriptor.Builder()
                    .name("Properties File")
                    .description("A Java-style Properties file to be used when loading CSV data into Stardog. The " +
                                 "property keys are identical to those used by the virtual import admin CLI. " +
                                 "Properties that are set in this file will supercede the individual processor " +
                                 "property values.")
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

    public static final PropertyDescriptor CSV_SEPARATOR =
            new PropertyDescriptor.Builder()
                    .name("CSV Separator")
                    .description("The character for separating fields in a delimited file. Defaults to comma (,).")
                    .required(false)
                    .defaultValue(",")
                    .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                    .addValidator(CHARACTER_VALIDATOR)
                    .build();

    public static final PropertyDescriptor CSV_QUOTE =
            new PropertyDescriptor.Builder()
                    .name("CSV Quote")
                    .description("The character used to enclose fields in a delimited file. Used for strings that " +
                                 "contain field separator characters. To escape a CSV Quote character within a " +
                                 "string that is enclosed in CSV Quote characters, use two consecutive CSV Quote " +
                                 "characters. Defaults to double quote (\").")
                    .required(false)
                    .defaultValue("\"")
                    .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                    // TODO: Fix #7911 - Allow no quote character for csv import
//                    .addValidator(NULLABLE_CHARACTER_VALIDATOR) // Adapt CHARACTER_VALIDATOR to optionally allow 0-length
                    .addValidator(CHARACTER_VALIDATOR)
                    .build();

    public static final PropertyDescriptor CSV_ESCAPE =
            new PropertyDescriptor.Builder()
                    .name("CSV Escape")
                    .description("Character for escaping special characters in delimited files. Used as an " +
                                 "alternative to the CSV Quote character. Defaults to unset.")
                    .required(false)
                    .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                    .addValidator(CHARACTER_VALIDATOR)
                    .build();

    public static final PropertyDescriptor CSV_HEADER =
            new PropertyDescriptor.Builder()
                    .name("CSV Header")
                    .description("Whether the delimited input file has a header line at the beginning. Defaults to true.")
                    .required(false)
                    .defaultValue("true")
                    .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                    .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
                    .build();

    public static final PropertyDescriptor CSV_SKIP_EMPTY =
            new PropertyDescriptor.Builder()
                    .name("CSV Skip Empty")
                    .description("Whether to treat empty (zero-length) fields in delimited files as null. " +
                                 "Defaults to true.")
                    .required(false)
                    .defaultValue("true")
                    .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                    .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
                    .build();

    public static final PropertyDescriptor BASE_URI =
            new PropertyDescriptor.Builder()
                    .name("Base URI")
                    .description("The URI to use as a prefix for auto-generated CSV mappings.")
                    .required(false)
                    .defaultValue("http://api.stardog.com/")
                    .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                    .addValidator(StandardValidators.URI_VALIDATOR)
                    .build();

    public static final PropertyDescriptor CSV_CLASS =
            new PropertyDescriptor.Builder()
                    .name("CSV Class")
                    .description("The class, or rdf:type, to use for the subjects of each row. Used when " +
                                 "auto-generating mappings for delimited file import.")
                    .required(false)
                    .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                    .addValidator(StandardValidators.URI_VALIDATOR)
                    .build();

    public static final PropertyDescriptor UNIQUE_KEY_SETS =
            new PropertyDescriptor.Builder()
                    .name("Unique Key Sets")
                    .description("The set of columns that uniquely identify each row in a delimited file. Used to " +
                                 "construct the subject template when auto-generating a delimited file mapping.")
                    .required(false)
                    .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                    // TODO: Custom validator
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .build();

    private static final List<PropertyDescriptor> PROPERTIES =
            ImmutableList.<PropertyDescriptor>builder()
                    .addAll(DEFAULT_PROPERTIES)
                    .add(INPUT_FORMAT)
                    .add(MAPPINGS_FILE)
                    .add(PROPERTIES_FILE)
                    .add(TARGET_GRAPH)
                    .add(CLEAR_TARGET_GRAPH)
                    .add(CSV_SEPARATOR)
                    .add(CSV_QUOTE)
                    .add(CSV_ESCAPE)
                    .add(CSV_HEADER)
                    .add(CSV_SKIP_EMPTY)
                    .add(CSV_CLASS)
                    .add(BASE_URI)
                    .add(UNIQUE_KEY_SETS)
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
    protected void customValidate(ValidationContext validationContext, Set<ValidationResult> results) {
        PropertyValue inputFormatProperty = validationContext.getProperty(INPUT_FORMAT);
        if (inputFormatProperty.isSet() &&
            "CSV".equals(inputFormatProperty.getValue()) &&
            !validationContext.getProperty(MAPPINGS_FILE).isSet() &&
            !validationContext.getProperty(UNIQUE_KEY_SETS).isSet()) {
            results.add(new ValidationResult.Builder().valid(false)
                                                      .subject(UNIQUE_KEY_SETS.getDisplayName())
                                                      .explanation(UNIQUE_KEY_SETS.getDisplayName() +
                                                                   " must be set when " +
                                                                   MAPPINGS_FILE.getDisplayName() +
                                                                   " is not set and " +
                                                                   INPUT_FORMAT.getDisplayName() +
                                                                   " is CSV")
                                                      .build());
        }
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

                PropertyValue mappingsPath = context.getProperty(MAPPINGS_FILE).evaluateAttributeExpressions(inputFile);
                String mappingString = mappingsPath.isSet()
                                       ? Files2.toString(new File(mappingsPath.getValue()).toPath(), Charsets.UTF_8)
                                       : null;

                if (clearTargetGraph) {
                    connection.begin();
                    connection.remove().context(targetGraph);
                    connection.commit();
                }

                InputFileType fileType = inputFormat.equals(QueryResultFormats.JSON)
                                         ? InputFileType.JSON
                                         : InputFileType.DELIMITED;

                Properties properties =
                        PropertySetter.builder(context, inputFile)
                                      .setProperty(CSV_SEPARATOR, VirtualGraphOptions.CSV_SEPARATOR)
                                      .setProperty(CSV_QUOTE, VirtualGraphOptions.CSV_QUOTE)
                                      .setProperty(CSV_ESCAPE, VirtualGraphOptions.CSV_ESCAPE)
                                      .setProperty(CSV_HEADER, VirtualGraphOptions.CSV_HEADER)
                                      .setProperty(CSV_SKIP_EMPTY, VirtualGraphOptions.CSV_SKIP_EMPTY)
                                      .setProperty(BASE_URI, VirtualGraphOptions.BASE_URI)
                                      .setProperty(CSV_CLASS, VirtualGraphOptions.CSV_CLASS)
                                      .setProperty(UNIQUE_KEY_SETS, VirtualGraphOptions.UNIQUE_KEY_SETS)
                                      .build();

                PropertyValue propertiesPath = context.getProperty(PROPERTIES_FILE).evaluateAttributeExpressions(inputFile);
                if (propertiesPath.isSet()) {
                    Properties propsFromFile = new Properties();
                    try (FileInputStream is = new FileInputStream(propertiesPath.getValue())) {
                        propsFromFile.load(is);
                    }
                    properties.putAll(propsFromFile);
                }

                vgConn.importFile(mappingString, properties, connection.name(), targetGraph, in, fileType);
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

    static class PropertySetter {
        private final ProcessContext mContext;
        private final FlowFile mInputFile;
        private final Properties mProperties = new Properties();

        private PropertySetter(ProcessContext context, FlowFile inputFile) {
            mContext = context;
            mInputFile = inputFile;
        }

        static PropertySetter builder(ProcessContext context, FlowFile inputFile) {
            return new PropertySetter(context, inputFile);
        }

        PropertySetter setProperty(PropertyDescriptor descriptor, String propertyName) {
            PropertyValue propertyValue = mContext.getProperty(descriptor).evaluateAttributeExpressions(mInputFile);
            if (propertyValue.isSet()) {
                String value = propertyValue.getValue();
                // org.apache.nifi.attribute.expression.language.StandardPreparedQuery.evaluateExpressions sets null results to ""
                if (!value.isEmpty()) {
                    mProperties.setProperty(propertyName, value);
                }
            }
            return this;
        }

        Properties build() {
            return mProperties;
        }
    }
}
