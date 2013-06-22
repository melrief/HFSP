package org.apache.hadoop.conf;

import org.apache.hadoop.conf.ConfigurationDescriptionToXMLConverter;

public interface AcceptConfigurationManagerVisitor extends Configurable {
    void accept(ConfigurationDescriptionToXMLConverter visitor);
}
