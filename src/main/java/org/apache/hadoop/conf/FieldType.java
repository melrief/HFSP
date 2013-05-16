package org.apache.hadoop.conf;

import java.util.Collection;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration.IntegerRanges;
import org.apache.hadoop.conf.ConfigurationDescription.BooleanConfiguration;
import org.apache.hadoop.conf.ConfigurationDescription.ClassConfiguration;
import org.apache.hadoop.conf.ConfigurationDescription.EnumConfiguration;
import org.apache.hadoop.conf.ConfigurationDescription.FloatConfiguration;
import org.apache.hadoop.conf.ConfigurationDescription.IntConfiguration;
import org.apache.hadoop.conf.ConfigurationDescription.IntegerRangesConfiguration;
import org.apache.hadoop.conf.ConfigurationDescription.LongConfiguration;
import org.apache.hadoop.conf.ConfigurationDescription.StringCollectionConfiguration;
import org.apache.hadoop.conf.ConfigurationDescription.StringConfiguration;

public class FieldType<T> {
  
  static HashMap<FieldType<?>, Class<? extends ConfigurationDescription<?>>> saferRegisteredClasses = new HashMap<FieldType<?>, Class<? extends ConfigurationDescription<?>>>();
  static {
    saferRegisteredClasses.put(new FieldType<Class>(Class.class), ClassConfiguration.class);
    //    saferRegisteredClasses.put(new FieldType<Enum>(Enum.class), (Class<? extends ConfigurationDescription<?>>) EnumConfiguration.class);
    saferRegisteredClasses.put(new FieldType<Collection>(Collection.class), StringCollectionConfiguration.class);
  }
  
  public final static FieldType<Boolean> Boolean = registerNewConfiguration(Boolean.class, BooleanConfiguration.class);
  //public final static FieldType<Class> Class = registerNewConfiguration(Class.class, ClassConfiguration.class);
  //public final static FieldType<Enum> Enum = registerNewConfiguration(Enum.class, EnumConfiguration.class);
  public final static FieldType<Integer> Integer = registerNewConfiguration(Integer.class, IntConfiguration.class);
  public final static FieldType<IntegerRanges> IntegerRanges = registerNewConfiguration(IntegerRanges.class, IntegerRangesConfiguration.class);
  public final static FieldType<Long> Long = registerNewConfiguration(Long.class, LongConfiguration.class);
  public final static FieldType<Float> Float = registerNewConfiguration(Float.class, FloatConfiguration.class);
  public final static FieldType<String> String = registerNewConfiguration(String.class, StringConfiguration.class);
  //public final static FieldType<Collection> Collection = registerNewConfiguration(Collection.class, StringCollectionConfiguration.class);
  
  public static <T1> FieldType<T1> registerNewConfiguration(
      Class<T1> cls, Class<? extends ConfigurationDescription<T1>> confDescription) {
    if (cls == null || confDescription == null) {
      throw new NullPointerException();
    }
    FieldType<T1> cc = new FieldType<T1>(cls);
    saferRegisteredClasses.put(cc, confDescription);
    return cc;
  }
  
  private Class<T> cls;

  private FieldType(Class<T> cls) {
    this.cls = cls;
  }
}
