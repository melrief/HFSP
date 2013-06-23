package org.apache.hadoop.conf;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration.IntegerRanges;

/** Describe a single key-value configuration that can be retrieved using the
 * {@link org.apache.hadoop.conf.ConfigurationDescription#get(Configuration)} 
 * from a {@link org.apache.hadoop.conf.Configuration}.
 * A configuration is composed by a key, the configuration description, 
 * the value type and a default value
 * 
 * @param <T> The type of the value
 */
public abstract class ConfigurationDescription<T> {

  private final String key;
  private final String description;
  private final T defaultValue;

  public ConfigurationDescription(String key, String description, T defaultValue) {
    if (key == null || description == null) {
      throw new NullPointerException();
    }
    this.key = key;
    this.description = description;
    this.defaultValue = defaultValue;
  }

  public static <T1> ConfigurationDescription<T1> from(FieldType<T1> cls,
      String key, String description, T1 defaultValue)
      throws IllegalArgumentException, InstantiationException,
      IllegalAccessException, InvocationTargetException {
    Class<?> helperCls = FieldType.saferRegisteredClasses.get(cls);
    Constructor<?> constructor = helperCls.getDeclaredConstructors()[0];
    return (ConfigurationDescription<T1>) constructor.newInstance(key,
        description, defaultValue);
  }

  // @SuppressWarnings("unchecked")
  // public static <T1> ConfigurationDescription<T1> from(Class<T1> cls,
  // String key, String description, T1 defaultValue)
  // throws IllegalArgumentException, InstantiationException,
  // IllegalAccessException, InvocationTargetException {
  // Class<?> helperCls = registeredClasses.get(cls);
  // Constructor<?> constructor = helperCls.getDeclaredConstructors()[0];
  // return (ConfigurationDescription<T1>) constructor.newInstance(key,
  // description, defaultValue);
  // }

  public final String getType() {
    return defaultValue.getClass().getSimpleName();
  }

  public String getKey() {
    return this.key;
  }

  public String getDescription() {
    return this.description;
  }

  public T getDefaultValue() {
    return this.defaultValue;
  }

  public String toPrettyString() {
    return this.key + " (type: " + this.getType() + ", default: "
        + this.getDefaultValue() + "): " + this.getDescription();
  }

  abstract protected T get(Configuration conf);

  public T checkAndGet(Configuration conf) {
    if (null == conf.get(this.getKey())) {
      System.out.println("WARN: configuration key '" + this.getKey()
          + "' not found, set it to default " + this.getDefaultValue());
    }
    return this.get(conf);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((key == null) ? 0 : key.hashCode());
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
    @SuppressWarnings("unchecked")
    ConfigurationDescription<T> other = (ConfigurationDescription<T>) obj;
    if (key == null) {
      if (other.key != null)
        return false;
    } else if (!key.equals(other.key))
      return false;
    return true;
  }

  static class BooleanConfiguration extends ConfigurationDescription<Boolean> {

    public BooleanConfiguration(String key, String description,
        Boolean defaultValue) {
      super(key, description, defaultValue);
    }

    @Override
    protected Boolean get(Configuration conf) {
      return conf.getBoolean(this.getKey(), this.getDefaultValue());
    }

  }

  static class ClassConfiguration extends ConfigurationDescription<Class<?>> {

    public ClassConfiguration(String key, String description,
        Class<?> defaultValue) {
      super(key, description, defaultValue);
    }

    @Override
    protected Class<?> get(Configuration conf) {
      return conf.getClass(this.getKey(), this.getDefaultValue());
    }

  }

  static class IntConfiguration extends ConfigurationDescription<Integer> {

    public IntConfiguration(String key, String description, Integer defaultValue) {
      super(key, description, defaultValue);
    }

    @Override
    protected Integer get(Configuration conf) {
      return conf.getInt(this.getKey(), this.getDefaultValue());
    }

  }

  static class EnumConfiguration<T extends Enum<T>> extends
      ConfigurationDescription<T> {

    public EnumConfiguration(String key, String description, T defaultValue) {
      super(key, description, defaultValue);
    }

    @Override
    protected T get(Configuration conf) {
      return conf.getEnum(this.getKey(), this.getDefaultValue());
    }

  }

  static class FloatConfiguration extends ConfigurationDescription<Float> {

    public FloatConfiguration(String key, String description, Float defaultValue) {
      super(key, description, defaultValue);
    }

    @Override
    protected Float get(Configuration conf) {
      return conf.getFloat(this.getKey(), this.getDefaultValue());
    }

  }

  static class LongConfiguration extends ConfigurationDescription<Long> {

    public LongConfiguration(String key, String description, Long defaultValue) {
      super(key, description, defaultValue);
    }

    @Override
    protected Long get(Configuration conf) {
      return conf.getLong(this.getKey(), this.getDefaultValue());
    }

  }

  static class StringConfiguration extends ConfigurationDescription<String> {

    public StringConfiguration(String key, String description,
        String defaultValue) {
      super(key, description, defaultValue);
    }

    @Override
    protected String get(Configuration conf) {
      return conf.get(this.getKey(), this.getDefaultValue());
    }

  }

  static class StringCollectionConfiguration extends
      ConfigurationDescription<Collection<String>> {

    public StringCollectionConfiguration(String key, String description,
        Collection<String> defaultValue) {
      super(key, description, defaultValue);
    }

    @Override
    protected Collection<String> get(Configuration conf) {
      Collection<String> value = conf.getStringCollection(this.getKey());
      return value.isEmpty() ? this.getDefaultValue() : value;
    }

  }

  static class IntegerRangesConfiguration extends
      ConfigurationDescription<IntegerRanges> {

    public IntegerRangesConfiguration(String key, String description,
        IntegerRanges defaultValue) {
      super(key, description, defaultValue);
    }

    @Override
    protected IntegerRanges get(Configuration conf) {
      return conf.getRange(this.getKey(), this.getDefaultValue().toString());
    }

  }
}