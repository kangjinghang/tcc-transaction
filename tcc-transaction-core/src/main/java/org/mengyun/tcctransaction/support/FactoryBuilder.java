package org.mengyun.tcctransaction.support;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by changming.xie on 2/23/17. 工厂 Builder
 */
public final class FactoryBuilder {

    // Bean 工厂集合
    private static List<BeanFactory> beanFactories = new ArrayList<BeanFactory>();
    private static ConcurrentHashMap<Class, SingeltonFactory> classFactoryMap = new ConcurrentHashMap<Class, SingeltonFactory>(); // 类 与 Bean工厂 的映射

    private FactoryBuilder() {

    }
    // 获得指定类单例工厂
    public static <T> SingeltonFactory<T> factoryOf(Class<T> clazz) {
        // 优先从 Bean 工厂集合 获取
        if (!classFactoryMap.containsKey(clazz)) {

            for (BeanFactory beanFactory : beanFactories) {
                if (beanFactory.isFactoryOf(clazz)) {
                    classFactoryMap.putIfAbsent(clazz, new SingeltonFactory<T>(clazz, beanFactory.getBean(clazz)));
                }
            }
            // 查找不到，创建 SingletonFactory
            if (!classFactoryMap.containsKey(clazz)) {
                classFactoryMap.putIfAbsent(clazz, new SingeltonFactory<T>(clazz));
            }
        }

        return classFactoryMap.get(clazz);
    }
    // 将 Bean工厂 注册到当前 Builder
    public static void registerBeanFactory(BeanFactory beanFactory) {
        beanFactories.add(beanFactory);
    }
    // 单例工厂
    public static class SingeltonFactory<T> {
        // 单例
        private volatile T instance = null;
        // 类名
        private String className;

        public SingeltonFactory(Class<T> clazz, T instance) {
            this.className = clazz.getName();
            this.instance = instance;
        }

        public SingeltonFactory(Class<T> clazz) {
            this.className = clazz.getName();
        }
        // 获得单例
        public T getInstance() {

            if (instance == null) {  // 不存在时，创建单例
                synchronized (SingeltonFactory.class) {
                    if (instance == null) {
                        try {
                            ClassLoader loader = Thread.currentThread().getContextClassLoader();

                            Class<?> clazz = loader.loadClass(className);

                            instance = (T) clazz.newInstance();
                        } catch (Exception e) {
                            throw new RuntimeException("Failed to create an instance of " + className, e);
                        }
                    }
                }
            }

            return instance;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) return true;
            if (other == null || getClass() != other.getClass()) return false;

            SingeltonFactory that = (SingeltonFactory) other;

            if (!className.equals(that.className)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return className.hashCode();
        }
    }
}