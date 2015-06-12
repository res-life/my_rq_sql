package com.yeahmobi.datasystem.query.guice;
/**
 * Created by yangxu on 5/7/14.
 */

import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.io.Closeables;
import com.google.inject.Binder;
import com.google.inject.Module;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.Properties;

public class PropertiesModule implements Module
{
    private static Logger logger = Logger.getLogger(PropertiesModule.class);

    private final String propertiesFile;

    public PropertiesModule(String propertiesFile)
    {
        this.propertiesFile = propertiesFile;
    }

    @Override
    public void configure(Binder binder)
    {
        final Properties fileProps = new Properties();
        Properties systemProps = System.getProperties();

        Properties props = new Properties(fileProps);
        props.putAll(systemProps);

        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        
        // first find the properties file from class loader resources
        InputStream stream = classloader.getResourceAsStream(propertiesFile);
        
        try {
            if (stream == null) {
                logger.error("Unable to load properties from " + propertiesFile);
                
                // if not find, load the file defined by "realquery.properties.file" system properties
                // if the system propertiy is null, use the default value, load from the running directory
                // TODO refactor
                File workingDirectoryFile = new File(systemProps.getProperty("realquery.properties.file", propertiesFile));
                if (workingDirectoryFile.exists()) {
                    stream = new BufferedInputStream(new FileInputStream(workingDirectoryFile));
                }
            }

            if (stream != null) {
                logger.info("Loading properties from " + propertiesFile);
                try {
                    fileProps.load(new InputStreamReader(stream, Charsets.UTF_8));
                }
                catch (IOException e) {
                    throw Throwables.propagate(e);
                } finally {
                    Closeables.closeQuietly(stream);
                }
            }
        }
        catch (FileNotFoundException e) {
            logger.error("This can only happen if the .exists() call lied.  That's f'd up.", e);
        }
        finally {
            Closeables.closeQuietly(stream);
        }

        binder.bind(Properties.class).toInstance(props);
    }
}
