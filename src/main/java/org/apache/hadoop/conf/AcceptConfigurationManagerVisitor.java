package org.apache.hadoop.conf;

import org.apache.hadoop.conf.ConfigurationDescriptionToXMLConverter;

/*
 * An interface for classes accepting a configuration description visitor
 */
public interface AcceptConfigurationManagerVisitor extends Configurable {
  // TODO: abstract from the type of visitor (not only XML converter)
  void accept(ConfigurationDescriptionToXMLConverter visitor);
}
