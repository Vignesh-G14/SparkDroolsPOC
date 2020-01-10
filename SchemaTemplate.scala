package com.hcsc.poc.sparkdrools.driver

import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import reh.dnoa.model.Provider
import reh.dnoa.model.Coverage
import reh.dnoa.model.Member
import reh.dnoa.model.Policy
import org.apache.spark.sql.types._

object SchemaTemplate {
  
     //M_MBR.grp_nbr,M_MBR.SUB_ID,M_MBR.PLCY_FAM_COMPNT_CD,M_MBR.MBR_NBR,M_MBR.LST_NM,
     //M_MBR.FST_NM,M_MBR.MID_NM,M_MBR.DOB,M_MBR.RELSHP_CD,M_MBR.GNDR_CD,
     //M_MBR.COVRG_EFF_DT,M_MBR.COVRG_END_DT,M_MBR.PD_THRU_DT,M_MBR.POSTL_ADDR_LN_1_NM,M_MBR.POSTL_ADDR_LN_2_NM,
     //M_MBR.CTY_NM,M_MBR.ST_CD,M_MBR.ZIP_CD,M_MBR.HOM_PHN_NBR,M_MBR.sect_nbr,
     //M_MBR.corp_ent_cd,M_MBR.SRC_SYS_NM,M_MBR.QLFY_HLTH_PLN_ID,M_MBR.LOB_CD
     import scala.collection.JavaConverters._
     def getStructuredPolicyObj(rowVal:Row):Policy={
                                                                                                                                                                                                
       val policy = new Policy(null,null,                                                                                                                                                       
                              // List(member).asJava,                                                                                                                                           
                              // rowVal.getList(rowVal.fieldIndex("members")).asScala.asJava,                                                                                                   
                               getMembersObjects(rowVal.getSeq[Row]((rowVal.fieldIndex("Members")))).asJava,                                                                                    
                               null,                                                                                                                                                            
                               (rowVal.getString(rowVal.fieldIndex("SUB_ID"))),                                                                                                                 
                               null,                                                                                                                                                            
                               rowVal.getString(rowVal.fieldIndex("corp_ent_cd")),                                                                                                              
                              null)                                                                                                                                                             
                                                                                                                                                                                                
       policy                                                                                                                                                                                   
                                                                                                                                                                                                
     }                                                                                                                                                                                          
                                                                                                                                                                                                
     def getMembersObjects(members: Seq[Row]) : List[Member] =
     {
       val membersobjlist = members.map(rowVal =>
                                    new Member((rowVal.getString(rowVal.fieldIndex("MBR_NBR"))),
                                        rowVal.getString(rowVal.fieldIndex("LST_NM")),
                                        rowVal.getString(rowVal.fieldIndex("FST_NM")),
                                        rowVal.getString(rowVal.fieldIndex("MID_NM")),
                                        null,
                                        (rowVal.getString(rowVal.fieldIndex("DOB"))),
                                        null,null,
                                        null,
                                        rowVal.getString(rowVal.fieldIndex("RELSHP_CD")),
                                        rowVal.getString(rowVal.fieldIndex("GNDR_CD")),
                                        (rowVal.getString(rowVal.fieldIndex("HOM_PHN_NBR"))),
                                        // "0",
                                        null,
                                        rowVal.getString(rowVal.fieldIndex("POSTL_ADDR_LN_1_NM")),
                                        rowVal.getString(rowVal.fieldIndex("POSTL_ADDR_LN_2_NM")),
                                        rowVal.getString(rowVal.fieldIndex("CTY_NM")),
                                        rowVal.getString(rowVal.fieldIndex("ST_CD")),
                                        rowVal.getString(rowVal.fieldIndex("ZIP_CD")),
                                        null,null,null,null,null,
                                        // List(coverage).asJava,
                                        getCoveragesObjects(rowVal.getSeq[Row]((rowVal.fieldIndex("Coverages")))).asJava,
                                        rowVal.getString(rowVal.fieldIndex("MBR_NBR")),null,null))
                                        membersobjlist.toList
     }
     
     def getCoveragesObjects(coverages: Seq[Row]) : List[Coverage] = 
     {
       val coveragesobjlist = coverages.map(rowVal =>
                                        new Coverage(null,null,
                                            rowVal.getString(rowVal.fieldIndex("COVRG_EFF_DT")),
                                            rowVal.getString(rowVal.fieldIndex("COVRG_END_DT")),
                                            null,
                                            rowVal.getString(rowVal.fieldIndex("PD_THRU_DT")),
                                            null,null,null,null,0,null,null,0,null,null,0,null,null,
                                            null,List(provider).asJava,
                                            rowVal.getString(rowVal.fieldIndex("grp_nbr")),
                                            rowVal.getString(rowVal.fieldIndex("PLCY_FAM_COMPNT_CD")),
                                            rowVal.getString(rowVal.fieldIndex("grp_nbr")),
                                            rowVal.getString(rowVal.fieldIndex("sect_nbr")),null))
                                            coveragesobjlist.toList
     }
     
     def getProvidersObjects(providers: Seq[Row]) : List[Provider] = 
     {
       val providersobjlist = providers.map(rowVal =>
                                        new Provider(null,null,null,null,null,null,null,null))
                                        providersobjlist.toList    
     }
     val provider = new Provider(null,null,null,null,null,null,null,null)

     
}
