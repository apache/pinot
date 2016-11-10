package com.linkedin.thirdeye.client.diffsummary.TeradataClient;

import java.util.List;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;


public class TeradataConnection {
  public static void main( String[ ] args ) {
    EntityManagerFactory emfactory = Persistence.createEntityManagerFactory( "Teradata_JPA" );
    EntityManager entitymanager = emfactory.createEntityManager();
    entitymanager.getTransaction().begin();
    Query query = entitymanager.createQuery("Select degree_name from dwh.v_dim_degree");
    List<String> list = query.getResultList();

    list.forEach(System.out::println);
    entitymanager.close();
    emfactory.close();
  }
}
