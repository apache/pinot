package com.linkedin.thirdeye.db;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.context.internal.ManagedSessionContext;

import java.util.concurrent.Callable;

public class HibernateSessionWrapper<V> {
  private final SessionFactory sessionFactory;

  public HibernateSessionWrapper(SessionFactory sessionFactory) {
    this.sessionFactory = sessionFactory;
  }

  public V execute(Callable<V> callable) throws Exception {
    Session session = sessionFactory.openSession();
    Transaction transaction = session.beginTransaction();
    try {
      ManagedSessionContext.bind(session);
      V result = callable.call();
      transaction.commit();
      return result;
    } catch (Exception e) {
      transaction.rollback();
      throw e;
    } finally {
      session.close();
      ManagedSessionContext.unbind(sessionFactory);
    }
  }
}
