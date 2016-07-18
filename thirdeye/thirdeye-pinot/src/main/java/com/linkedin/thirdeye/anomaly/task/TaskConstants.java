package com.linkedin.thirdeye.anomaly.task;

public class TaskConstants {

  public enum TaskType {
    AnomalyDetection {

      @Override
      Class<?> getKlazz() {
        return AnomalyDetectionTaskRunner.class;
      }

    },
    Merge {

      @Override
      Class<?> getKlazz() {
        // TODO Auto-generated method stub
        return null;
      }

    },
    Reporter {

      @Override
      Class<?> getKlazz() {
        // TODO Auto-generated method stub
        return null;
      }

    },
    Monitor {

      @Override
      Class<?> getKlazz() {
        // TODO Auto-generated method stub
        return null;
      }

    };

    abstract Class<?> getKlazz();

    String getName() {
      return this.name().toLowerCase();
    }
  }

  public enum TaskStatus {
    WAITING,
    RUNNING,
    COMPLETED,
    FAILED
  };

}
