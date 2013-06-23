package org.apache.hadoop.conf;

import java.lang.reflect.InvocationTargetException;
import java.util.HashSet;

/**
 * This class simplify the configuration using Apache Configuration. An instance
 * can be create using {@link ConfigurationManager#createFor(Object)}. Any
 * object can use an instance of this class parametrized on itself to:
 * <ul>
 * <li>declare how it can be configured using
 * {@link ConfigurationManager#addConfiguratorFor(Class, String, String, Object, Configurator)},
 * {@link ConfigurationManager#addConfiguratorForOrFalse(Class, String, String, Object, Configurator)}
 * or
 * {@link ConfigurationManager#addConfiguratorAndConfiguration(Configurator, ConfigurationDescription)}
 * </li>
 * <li>get a string representing the current configuration using
 * {@link ConfigurationManager#toString()}</li>
 * <li>configure the target by passing a {@link Configuration} to
 * {@link ConfigurationManager#configure(Configuration)}</li>
 * </ul>
 * 
 * ConfigurationManager can be used in any class extending {@link Configurable}
 * by overriding the method {@link Configurable#setConf(Configuration)} and
 * adding a call to {@link ConfigurationManager#configure(Configuration)}:
 * 
 * <pre>
 * {@code
 * ConfigurationManager cm = ConfigurationManager.createFrom(this);
 * 
 * public void setConf(Configuration conf) {
 *  super.setConf(conf);
 *  if (conf != null) {
 *    this.cm.configure(this.getConf());
 *  }
 * }
 * }
 * </pre>
 * 
 * @param <O>
 *          the class to configure
 */
public class ConfigurationManager<O> {
  private O toConfigure;

  HashSet<ConfiguratorConfiguration<?, O>> configuratorConfigurations = new HashSet<ConfiguratorConfiguration<?, O>>();

  private ConfigurationManager(O toConfigure) {
    this.toConfigure = toConfigure;
  }

  public static <O> ConfigurationManager<O> createFor(O toConfigure) {
    if (toConfigure == null) {
      throw new NullPointerException(
          "cannot create a configuration manager for null");
    }
    return new ConfigurationManager<O>(toConfigure);
  }

  public void accept(ConfigurationDescriptionToXMLConverter converter) {
    for (ConfiguratorConfiguration<?, O> configuratorConfiguration : this.configuratorConfigurations) {
      converter
          .addConfigurationDescription(configuratorConfiguration.configuration);
    }
  }

  public void configure(Configuration conf) {
    for (ConfiguratorConfiguration<?, O> configuratorConfiguration : this.configuratorConfigurations) {
      configuratorConfiguration.configure(this.toConfigure, conf);
    }
  }

  public <T> void addConfiguratorFor(FieldType<T> cls, String key,
      String description, T defaultValue, Configurator<T, O> configurator)
      throws IllegalArgumentException, InstantiationException,
      IllegalAccessException, InvocationTargetException {
    this.addConfiguratorAndConfiguration(configurator,
        ConfigurationDescription.from(cls, key, description, defaultValue));
  }

  // public <T> void addConfiguratorFor(Class<T> cls, String key,
  // String description, T defaultValue, Configurator<T, O> configurator)
  // throws IllegalArgumentException, InstantiationException,
  // IllegalAccessException, InvocationTargetException {
  // this.addConfiguratorAndConfiguration(configurator,
  // ConfigurationDescription.from(cls, key, description, defaultValue));
  // }

  public <T> boolean addConfiguratorForOrFalse(FieldType<T> cls, String key,
      String description, T defaultValue, Configurator<T, O> configurator) {
    try {
      this.addConfiguratorFor(cls, key, description, defaultValue, configurator);
      return true;
    } catch (IllegalArgumentException e) {
      return false;
    } catch (InstantiationException e) {
      return false;
    } catch (IllegalAccessException e) {
      return false;
    } catch (InvocationTargetException e) {
      return false;
    }
  }

  public <T> void addConfiguratorAndConfiguration(
      Configurator<T, O> configurator, ConfigurationDescription<T> configuration) {
    this.configuratorConfigurations.add(new ConfiguratorConfiguration<T, O>(
        configuration, configurator));
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder("Configuration keys for "
        + toConfigure.getClass() + ":");
    for (ConfiguratorConfiguration<?, ?> cc : this.configuratorConfigurations) {
      builder.append("\n").append(" ")
          .append(cc.configuration.toPrettyString());
    }
    return builder.toString();
  }
}

class ConfiguratorConfiguration<T, O> {
  ConfigurationDescription<T> configuration;
  Configurator<T, O> configurator;

  public ConfiguratorConfiguration(ConfigurationDescription<T> configuration,
      Configurator<T, O> configurator) {
    this.configuration = configuration;
    this.configurator = configurator;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result
        + ((configuration == null) ? 0 : configuration.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    @SuppressWarnings("rawtypes")
    ConfiguratorConfiguration other = (ConfiguratorConfiguration) obj;
    if (configuration == null) {
      if (other.configuration != null)
        return false;
    } else if (!configuration.equals(other.configuration))
      return false;
    return true;
  }

  public void configure(O obj, Configuration conf) {
    this.configurator.configure(obj, this.configuration, conf);
  }
}

class ExampleObj {

  ConfigurationManager<ExampleObj> cm = ConfigurationManager.createFor(this);

  public ExampleObj() throws IllegalArgumentException, InstantiationException,
      IllegalAccessException, InvocationTargetException {
    this.cm.addConfiguratorFor(FieldType.Integer, "intKey",
        "this is the description for configurator of the i field", -1,
        new Configurator<Integer, ExampleObj>() {
          protected void set(ExampleObj obj, Integer value) {
            obj.setI(value);
          }
        });
    this.cm.addConfiguratorFor(FieldType.Float, "floatKey", "description", -1f,
        new Configurator<Float, ExampleObj>() {
          protected void set(ExampleObj obj, Float value) {
            obj.setF(value);
          }
        });
  }

  private int i = 0;

  public void setI(int i) {
    this.i = i;
  }

  private float f = 0;

  public void setF(Float value) {
    this.f = value;
  }

  @Override
  public String toString() {
    return String.valueOf(this.i + " " + this.f);
  }

  public static void main(String[] args) throws IllegalArgumentException,
      InstantiationException, IllegalAccessException, InvocationTargetException {
    ExampleObj eo = new ExampleObj();
    System.out.println(eo.cm.toString());
    Configuration c = new Configuration();
    c.setInt("intKey", 20);
    c.setFloat("floatKey", 301f);
    eo.cm.configure(c);
    System.out.println(eo);
  }
}
