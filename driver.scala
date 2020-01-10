package com.hcsc.poc.sparkdrools.driver

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.kie.api.KieServices
import org.apache.spark.sql.Row
import org.slf4j.LoggerFactory
//import test.scalaprojecttest.PolicyObj
import org.apache.log4j.Logger
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.kie.api.KieBase
import org.kie.api.KieServices
import org.kie.api.runtime.StatelessKieSession
import org.apache.spark.sql.functions._

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import com.typesafe.config.{ConfigFactory, ConfigException }
import java.nio.file.Paths
import scala.util.Try
import scala.io.Source._
import org.apache.spark.sql.SQLContext
import reh.dnoa.model.Policy
import com.hcsc.poc.sparkdrools.drools.LoadRules
import org.apache.spark.sql.{Encoder,Encoders}
import scala.util.Success
import scala.util.Failure
import org.apache.spark.sql.SaveMode

//case class mypolicy(pol: Policy)

object SparkDroolsPocDriverDNOA {
  val logger = LogManager.getLogger(getClass.getName)
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]) = {
    Try {
      val sparkConf = new SparkConf().setAppName("Spark Drools POC").setMaster("yarn")

       // building spark session
      val spark = SparkSession
                   .builder
                   .config(sparkConf)
                   .enableHiveSupport()
                   .appName("Spark Drools POC").getOrCreate()
                   
      println("SparkContext and Session Created Successfully ...................")

      
      //Reading input data from Hive Table
      //val home = System.getProperty("user.home")
      val hiveDatabase = args(0)
      val queryFile = args(1)
      val outputPathCSV = args(2)
      //val outputPathText = args(3)

      val BRMSQuery = scala.io.Source.fromFile(queryFile).
                                      getLines.filterNot(_.isEmpty()).map{line => if(line.contains("$DATABASE")){line.replace("$DATABASE", hiveDatabase)}else{line}}.mkString(" ")

      println("Control-1")

      val inputDataFrame = spark.sql(BRMSQuery)
      inputDataFrame.printSchema()
                                                          
      /*//Reading input data from CSV file            
      val inputDF = spark.read.format("csv")
                          .option("header", "true")
                          //.option("inferSchema", "true")
                          .schema(SchemaTemplate.csvschema)
                          .option("sep", ";")
                          .load(inputPath)*/
        
      /*//Workaround solution for null pointer exception at index 18,value for PLCY_FAM_COMPNT_CD should not be null. BRMS is not accepting null for that column
      val inputDF = inputDF1.na.fill(0.0, Seq("PLCY_FAM_COMPNT_CD"))
      inputDF.show(false);*/
      println("Control-2")
      import spark.implicits._
      import org.apache.spark.sql.functions.{collect_list}
                
      val intermediateDF1= inputDataFrame.groupBy($"SUB_ID",$"corp_ent_cd",$"MBR_NBR",$"LST_NM",$"FST_NM",$"MID_NM",$"DOB",$"RELSHP_CD",$"GNDR_CD",$"POSTL_ADDR_LN_1_NM",
                                                  $"POSTL_ADDR_LN_2_NM",$"CTY_NM",$"ST_CD",$"ZIP_CD",$"HOM_PHN_NBR").
                                                  agg(collect_list(struct($"grp_nbr",$"PLCY_FAM_COMPNT_CD",$"COVRG_EFF_DT",$"COVRG_END_DT",$"PD_THRU_DT",$"sect_nbr")).as("Coverages"))
      val intermediateDF2=intermediateDF1.groupBy($"SUB_ID",$"corp_ent_cd").
                                                  agg(collect_list(struct($"MBR_NBR",$"LST_NM",$"FST_NM",$"MID_NM",$"DOB",$"RELSHP_CD",$"GNDR_CD",$"POSTL_ADDR_LN_1_NM",
                                                                          $"POSTL_ADDR_LN_2_NM",$"CTY_NM",$"ST_CD",$"ZIP_CD",$"HOM_PHN_NBR",$"Coverages")).as("Members"))
      intermediateDF2.show(false)
      
      println("Control-3")         
      import spark.implicits._
      import org.apache.spark.sql.Encoders._
                
      //To convert the RDD[Policy] to dataframe it need schema- By using Encoders we are directly imposing the Policy schema
      implicit val encoder = Encoders.bean(classOf[Policy])
      
      //Extract the input policy objects in json format
      //val BRMSRDD = intermediateDF2.rdd.map[Policy](row=>SchemaTemplate.getStructuredPolicyObj(row))
      //BRMSRDD.toDF().coalesce(1).write.mode(SaveMode.Overwrite).json(inputtojsonPath)
                
      //Giving our input to Drools as Policy java object,getting output as Rdd of policy
      println("number of records :"+policyBRMSRDD.count)
      val policyBRMSRDD = intermediateDF2.rdd
      val policyBRMSRDDParts = policyBRMSRDD.getNumPartitions
      println("Before BRMS :: number of partitions :"+policyBRMSRDDParts)
      //val policyBRMSDF = policyBRMSRDD.map[Policy]{policyRow => LoadRules.applyAllRules(SchemaTemplate.getStructuredPolicyObj(policyRow))}
      val policyBRMSDF = policyBRMSRDD.mapPartitionsWithIndex[Policy]({(itr,policyRow) => 
        policyRow.map(row => LoadRules.applyAllRules(SchemaTemplate.getStructuredPolicyObj(row),itr))
        }, false)
      val policyBRMSDFParts = policyBRMSDF.getNumPartitions
      println("After BRMS :: number of partitions :"+policyBRMSDFParts)
      
      //Conversion of RDD[policy] to Dataframe
      val a = policyBRMSDF.toDF()
      println("ResultDF is")
      a.show(false);
      a.printSchema()
                
      //After creating output rdd to Dataframe flattening the nested structure to individual columns by using explode.
      val membersexplode = a.select($"subscriberID",$"corporationEntityCode",$"grpState",$"memberID",$"memberGID",$"rulesTriggered",explode($"members").as("member"))
      membersexplode.show(false)
      membersexplode.printSchema()
      val coveragesexplode = membersexplode.select($"subscriberID",$"corporationEntityCode",$"grpState",$"memberID",$"memberGID",$"rulesTriggered",$"member",
                                            explode($"member.coverages").as("coverage"))
                
      //val resultDF = coveragesexplode.select($"subscriberID",$"corporationEntityCode",$"grpState",concat($"memberID", lit("   ")).alias("memberID"),$"memberGID",col("rulesTriggered").as("rulesTriggeredPolicy"),$"member.memberNumber",$"member.lastName",$"member.firstName",$"member.middleName",$"member.middleInitial",$"member.birthDate",$"member.relationshipCodeTarget",$"member.relationshipCodeSource",$"member.gender",$"member.homePhoneNumber",$"member.workPhoneNumber",$"member.homeAddress1",$"member.homeCity",$"member.homeState",$"member.homeZipCd",$"member.socialSecurityNumber",$"member.maritalStatus",$"coverage.coverageCode",$"coverage.coverageEffectiveDate",$"coverage.coverageEndDateSource",$"coverage.coverageEndDateTarget",$"coverage.paidThruDate",$"coverage.payThruDate",$"coverage.pendThruDate",col("coverage.rulesTriggered").as("rulesTriggeredCoverage"),$"coverage.grpID",$"coverage.policyFamilyComponentCode",$"coverage.groupNumberSource",$"coverage.sectionNumberSource",$"coverage.primaryDentistContactID",concat($"member.memberNumberTarget",lit(" ")).alias("memberNumberTarget"),col("member.rulesTriggered").as("rulesTriggeredMember"))
                
      //memberID,membernumberTarget need to have spaces follwed by column value
      val resultDF = coveragesexplode.select($"coverage.grpID",$"memberGID",concat($"memberID",lit("   ")).alias("memberID"),
                                      $"coverage.coverageCode",concat($"member.memberNumberTarget",lit(" ")).alias("memberNumberTarget"),$"member.lastName",
                                      $"member.firstName",$"member.middleInitial",$"member.birthDate",$"member.maritalStatus",$"member.socialSecurityNumber",
                                      $"member.relationshipCodeTarget",$"member.gender",$"coverage.coverageEffectiveDate",$"coverage.coverageEndDateTarget",
                                      $"coverage.paidThruDate",$"member.homeAddress1",$"member.homeAddress2",$"member.homeCity",$"member.homeState",$"member.homeZipCd",
                                      $"member.homePhoneNumber",$"member.workPhoneNumber",$"coverage.subGroup",$"coverage.primaryDentistContactID",$"grpState",$"coverage.payThruDate",
                                      $"coverage.pendThruDate")
                
      resultDF.show(false);
      resultDF.printSchema();
      
      /*val limit2732 = policyBRMSDFPolicy.limit(2732)
      val limit2731 = limit2732.limit(2731)
      val malformedDF = limit2732.except(limit2731)*/
              
      //Saving dataframe to csv
      resultDF.write.format("csv").option("header", "true").option("sep", ";").mode(SaveMode.Overwrite).save(outputPathCSV)
      //resultDF.rdd.map(_.toString()).saveAsTextFile(outputPathText)
      //resultOne.write.mode(SaveMode.Overwrite).saveAsTable("reh_db.POCONE")
      
      }
    match{
      case Success(k) => logger.info("Execution is success")
      case Failure(_: ConfigException.Missing) =>{
        System.err.println("Please provide all parameters in configuration file")
        System.exit(1)
        }
      case Failure(e) => {
        logger.error("Exception :" + e.toString)
        logger.error("Exception :" + e.printStackTrace())
        logger.error("Exception :" + e.getMessage())        
        System.exit(1)
        }
    }
  }
  
  
}
