package com.swayam.storm.jms;

import javax.jms.ConnectionFactory;
import javax.jms.Destination;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import org.apache.storm.jms.JmsProvider;

@SuppressWarnings("serial")
public class SpringJmsProvider implements JmsProvider {
	 private ConnectionFactory connectionFactory;
     private Destination destination;

	public  SpringJmsProvider(String appContextClasspathResource, String connectionFactoryBean, String destinationBean){
         ApplicationContext context = new ClassPathXmlApplicationContext(appContextClasspathResource);
         this.connectionFactory = (ConnectionFactory)context.getBean(connectionFactoryBean);
         this.destination = (Destination)context.getBean(destinationBean);
 }

	public ConnectionFactory connectionFactory() throws Exception {
		return this.connectionFactory;
	}

	public Destination destination() throws Exception {
		return this.destination;
	}

}
