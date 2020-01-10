package com.hcsc.poc.sparkdrools.drools

import org.kie.api.KieBase
import org.kie.api.KieServices
import org.kie.api.runtime.StatelessKieSession
import org.kie.internal.command.CommandFactory
import org.apache.spark.sql.functions._
import scala.reflect.api.materializeTypeTag

import org.apache.log4j.Logger
import org.apache.log4j.LogManager
import java.io.FileInputStream
import org.kie.api.builder.Message
import org.kie.api.io.ResourceType
import reh.dnoa.model.Policy
import org.apache.spark.sql.Encoders
import java.util.Date
import java.text.SimpleDateFormat



object LoadRules extends Serializable {
  def applyAllRules(p:Policy):Policy = {
    
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    
		println("Drools Engine API start : Timestamp :: "+format.format(new Date(System.currentTimeMillis())));  
    val kieServices = KieServices.Factory.get();
    val kieContainer = kieServices.getKieClasspathContainer();
    val kieSession = kieContainer.newKieSession();
    println("Session created: Timestamp :: "+format.format(new Date(System.currentTimeMillis())));
    try{
        kieSession.insert(p);
   	    kieSession.getAgenda().getAgendaGroup("DNOARulesGroup").setFocus();
   	    println(" Agenda group name :"+kieSession.getAgenda().getAgendaGroup("DNOARulesGroup").getName+" : Timestamp :: "+format.format(new Date(System.currentTimeMillis())))
        kieSession.fireAllRules();
       println("after fireallrules: Timestamp :: "+format.format(new Date(System.currentTimeMillis())))
  		  }catch{
    		  case e:Exception => println("Exception message :"+e.getMessage);
    		  println("Exception statck trace :"+e.getStackTrace)
    		  }
      println("new message after rule validation : "+p.toString +" : Timestamp :: "+format.format(new Date(System.currentTimeMillis())))
      return p
    }
  
   def applyAllRules(p:Policy,partitionNo:Int):Policy = {
    println("Executor partition number :: "+partitionNo)
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    
		println("Drools Engine API start : Timestamp :: "+format.format(new Date(System.currentTimeMillis())));  
    val kieServices = KieServices.Factory.get();
    val kieContainer = kieServices.getKieClasspathContainer();
    val kieSession = kieContainer.newKieSession();
    println("Session created: Timestamp :: "+format.format(new Date(System.currentTimeMillis())));
    try{
        kieSession.insert(p);
   	    kieSession.getAgenda().getAgendaGroup("DNOARulesGroup").setFocus();
   	    println(" Agenda group name :"+kieSession.getAgenda().getAgendaGroup("DNOARulesGroup").getName+" : Timestamp :: "+format.format(new Date(System.currentTimeMillis())))
        kieSession.fireAllRules();
       println("after fireallrules: Timestamp :: "+format.format(new Date(System.currentTimeMillis())))
  		  }catch{
    		  case e:Exception => println("Exception message :"+e.getMessage);
    		  println("Exception statck trace :"+e.getStackTrace)
    		  }
  		//println("new message after rule validation : "+p.toString )
      println("Drools Engine API end : Timestamp :: "+format.format(new Date(System.currentTimeMillis())))
      return p
    }
  

}
