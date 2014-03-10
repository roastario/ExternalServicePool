package com.stefano.service;

import org.apache.log4j.Logger;

import java.util.Map;
import java.util.concurrent.*;

/**
 * User: franzs
 * Date: 26/02/14
 * Time: 16:46
 */
public abstract class ExternalServiceConnectionPool<T extends AutoCloseable> {

    private static final Logger logger = Logger.getLogger(ExternalServiceConnectionPool.class);
    private final static ScheduledExecutorService EXECUTOR_SERVICE = Executors.newSingleThreadScheduledExecutor();
    private final long milliesToWait;
    private BlockingQueue<T> theServices = new LinkedBlockingQueue<>();
    private ConcurrentHashMap<T, TakeServiceInfo> infoMap = new ConcurrentHashMap<>();

    public ExternalServiceConnectionPool(int numberOfServices) {
        this(numberOfServices, 5, TimeUnit.SECONDS);
    }


    public ExternalServiceConnectionPool(int numberOfServices, long timeout, TimeUnit timeoutUnit) {

        for (int i = 0; i < numberOfServices; i++) {
            T theService = initialValue();
            infoMap.put(theService, TakeServiceInfo.AVAILABLE_SERVICE);
            theServices.add(theService);
        }
        milliesToWait = timeoutUnit.toMillis(timeout);
        EXECUTOR_SERVICE.scheduleWithFixedDelay(new CleaningRunnable(), 0, 30, TimeUnit.SECONDS);
    }

    protected abstract T initialValue();

    public void releaseService(T service) {
        logger.info(Thread.currentThread() + " has released: " + service.toString());
        if (infoMap.containsKey(service)) {
            infoMap.put(service, TakeServiceInfo.AVAILABLE_SERVICE);
            theServices.offer(service);
        }
    }

    public T getService() {
        try {
            T service = theServices.take();
            infoMap.put(service, new TakeServiceInfo());
            logger.info(Thread.currentThread() + " took service: " + service);
            return service;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void abandonService(T service) {
        try {
            if (infoMap.remove(service, TakeServiceInfo.ABANDON_SERVICE)) {
                service.close();
                T newService = initialValue();
                infoMap.put(newService, TakeServiceInfo.AVAILABLE_SERVICE);
                theServices.offer(newService);
            }
        } catch (Exception e) {
            //NOTHING WE CAN DO NOW!
            logger.info("Exception during close of service: ", e);
        }
    }

    private static class TakeServiceInfo {

        private static TakeServiceInfo ABANDON_SERVICE = new TakeServiceInfo();
        private static TakeServiceInfo AVAILABLE_SERVICE = new TakeServiceInfo();
        private Thread takingThread;
        private long timeTaken;

        private TakeServiceInfo() {
            this.takingThread = Thread.currentThread();
            this.timeTaken = System.currentTimeMillis();
        }
    }

    private class CleaningRunnable implements Runnable {

        @Override
        public void run() {
            for (Map.Entry<T, TakeServiceInfo> entry : infoMap.entrySet()) {
                if (entry.getValue() != TakeServiceInfo.AVAILABLE_SERVICE && entry.getValue() != TakeServiceInfo.ABANDON_SERVICE) {
                    TakeServiceInfo info = entry.getValue();
                    if ((System.currentTimeMillis() - info.timeTaken) >= milliesToWait) {
                        logger.info("Expiring: " + entry.getKey() + " as it has been held by thread: " + info.takingThread + " for over the timeout value");
                        infoMap.replace(entry.getKey(), info, TakeServiceInfo.ABANDON_SERVICE);
                    }
                }
                if (infoMap.get(entry.getKey()) == TakeServiceInfo.ABANDON_SERVICE) {
                    abandonService(entry.getKey());
                }
            }
        }
    }
}
