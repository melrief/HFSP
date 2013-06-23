package org.apache.hadoop.conf;

import org.apache.hadoop.conf.ConfigurationDescription;

abstract public class Configurator<T, O> {

  public void configure(O obj, ConfigurationDescription<T> configuration,
      Configuration conf) {
    T value = configuration.checkAndGet(conf);
    this.set(obj, value);
  }

  abstract protected void set(O obj, T value);
}
