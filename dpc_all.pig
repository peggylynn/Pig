--REGISTER '$UDFPath';
register /home/cloudera/data/piggybank/piggybank.jar;
DEFINE DiffDate org.apache.pig.piggybank.evaluation.datetime.DiffDate();

%default INITIAL FALSE;

define Over org.apache.pig.piggybank.evaluation.Over();
define Stitch org.apache.pig.piggybank.evaluation.Stitch();
define CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage();
DEFINE toString InvokeForString('java.net.URLDecoder.decode', 'String String');
DEFINE StringToLong InvokeForLong('java.lang.Long.valueOf', 'String');
DEFINE toLong InvokeForLong('java.lang.Long.valueOf', 'Long');
DEFINE StringToInt InvokeForLong('java.lang.Long.valueOf', 'int');
DEFINE ToString org.apache.pig.builtin.ToString;

-- set compression
--set output.compression.enabled 'true'
--set output.compression.codec com.hadoop.compression.lzo.LzopCodec;


--set pig.tmpfilecompression 'true'; 
--set pig.tmpfilecompression.codec 'lzo';


--set io.sort.mb 10000;
/* This is a simple version of the Pig script to start with a file that has two dates of data.  The data is from DEVICE_PAGE_COUNT so all the
preliminary code in the original version of the script is not needed.  We already have the surrogate keys by the point this script steps in. */

/*----------------------- Load test data unloaded from DEVICE_PAGE_COUNT --------------------------------------------------*/
--I will use the relation names Jeremy used, even though I'm not getting the data from UPSRECEIVE for this test.  It will make it easier to transition.

--This loads the intermediate file that was processed by Jeremy's pig script. 
--The file will be in LZO compressed format.  Need to replace the filename here with a parameter. 

devicePageCountAll = LOAD 'Jan_5-6_DPC_All.txt' USING PigStorage('\t')
                            AS (
							account_code:chararray,	
							serial_number:chararray,	
							reported_date:chararray,	
							lifetime_page_count:chararray,	
							color_lifetime_page_count:chararray,	
							mono_lifetime_page_count:chararray,	
							fax_lifetime_page_count:chararray,
							scan_lifetime_page_count:chararray,	
							color_copy_lifetime_page_count:chararray,	
							mono_copy_lifetime_page_count:chararray,
							total_copy_lifetime_page_count:chararray,	
							page_count:chararray,	
							color_page_count:chararray,	
							mono_page_count:chararray,	
							fax_page_count:chararray,	
							scan_page_count:chararray,	
							color_copy_page_count:chararray,	
							mono_copy_page_count:chararray,	
							total_copy_page_count:chararray,	
							days_since_last_reported:chararray,
							printer_name:chararray,	
							ip_address:chararray,	
							suspect_state:chararray,	
							data_source:chararray,
                                                	time_dimension_surrogate_key:chararray,	
							customer_master_surrogate_key:chararray,	
							product_master_surrogate_key:chararray,	
							material_master_surrogate_key:chararray,	
							account_surrogate_key:chararray,	
							asset_surrogate_key:chararray,	
							agreement_surrogate_key:chararray,
							create_date_time:chararray,	
							create_user:chararray,	
                                                        update_date_time:chararray,	
							update_user:chararray,
	                                                oldRecord:chararray
							);
describe devicePageCountAll;
--dump devicePageCountAll;

devicePageCountAll_distinct = DISTINCT devicePageCountAll;
--describe devicePageCountAll_distinct;
--dump devicePageCountAll_distinct;


/*---------------------------------------Generate Schema file for All Records------------------------------------------------*/
pageCountUpsReceive_ALL = FOREACH devicePageCountAll_distinct GENERATE  
                                            account_code, 
                                            serial_number,   
                                            reported_date, 
                                            StringToLong(lifetime_page_count) as lifetime_page_count,  
                                            StringToLong(color_lifetime_page_count) as color_lifetime_page_count, 
                                            StringToLong(mono_lifetime_page_count) as mono_lifetime_page_count, 
                                            StringToLong(fax_lifetime_page_count) as fax_lifetime_page_count, 
                                            StringToLong(scan_lifetime_page_count) as scan_lifetime_page_count, 
                                            StringToLong(color_copy_lifetime_page_count) as color_copy_lifetime_page_count, 
                                            StringToLong(mono_copy_lifetime_page_count) as mono_copy_lifetime_page_count, 
                                            StringToLong(total_copy_lifetime_page_count) as total_copy_lifetime_page_count, 
 					    StringToLong(page_count) AS page_count,
					    StringToLong(color_page_count) as color_page_count,
					    StringToLong(mono_page_count) as mono_page_count,
					    StringToLong(fax_page_count) as fax_page_count,
					    StringToLong(scan_page_count) as scan_page_count,
					    StringToLong(color_copy_page_count) as color_copy_page_count,
					    StringToLong(mono_copy_page_count) as mono_copy_page_count,
					    total_copy_page_count as total_copy_page_count,
					    0 as   days_since_last_reported, 
                                            printer_name, 
                                            ip_address, 
                                            suspect_state, 
                                            data_source,
                                            time_dimension_surrogate_key,
                                            customer_master_surrogate_key,
                                            StringToLong(product_master_surrogate_key) as product_master_surrogate_key,
                                            material_master_surrogate_key,
                                            account_surrogate_key,
                                            asset_surrogate_key,
                                            agreement_surrogate_key,
                                            ToString(CurrentTime(), 'yyyy-MM-dd HH:mm:ss') AS create_date_time,
                                            --'$USERNAME' AS create_user,
                                            'Peggy' AS create_user,
                                            ToString(CurrentTime(), 'yyyy-MM-dd HH:mm:ss') AS update_date_time,
                                            --'$USERNAME' AS update_user,
                                            'none' AS update_user,
                                             oldRecord;
describe pageCountUpsReceive_ALL;
--dump pageCountUpsReceive_ALL;


-----------------------------------This is ending up with NULL datatypes in the schema before the stitches -> that's normal....--------------------
--Do them just as separate relations and do a describe

--Example from Internet:
/*--launch pig with awareness of the HCatalog metastore;

pig -useHCatalog; 
register piggybank.jar 
define Stitch org.apache.pig.piggybank.evaluation.Stitch;
define Over org.apache.pig.piggybank.evaluation.Over; 
A = load 'nmdata' using org.apache.hcatalog.pig.HCatLoader(); 
B = group A by visid_high; 
C = foreach B { 
     C1 = order A by date_time; 
     generate FLATTEN(Stitch(C1, Over(C1.$3, 'count'))); 
}; 
DUMP C;
*/ --Note how the Stitch() function appears in the FLATTEN() function, not before.


 /*---------------------------------------Final Calculation Over Lag-----------------------------------------------------------------*/

groupUpsReceive_final = group pageCountUpsReceive_ALL by (account_code, serial_number);
describe groupUpsReceive_final;
--dump groupUpsReceive_final;

--For the following, the list of fields after the generate flatten(stitchUpsReceive) MUST be in the same order as groupUpsRecieve_final!!!!!!!!!!!!!!!
lagUpsReceive_final = foreach groupUpsReceive_final {
     orderUpsReceive_final = order pageCountUpsReceive_ALL by reported_date;
     LAG_RPT_DATE = Over(orderUpsReceive_final.reported_date, 'lag', 0, 1, 1, '1900-01-01 00:00:00');
     LAG_LIFE_PAGE_COUNT = Over(orderUpsReceive_final.lifetime_page_count, 'lag',0,1,1,0L);
     LAG_COLOR_LPC = Over(orderUpsReceive_final.color_lifetime_page_count, 'lag', 0, 1, 1,0L);
     LAG_MONO_LPC = Over(orderUpsReceive_final.mono_lifetime_page_count, 'lag', 0, 1, 1,0L);
     LAG_FAX_LPC = Over(orderUpsReceive_final.fax_lifetime_page_count, 'lag', 0, 1, 1,0L);
     LAG_SCAN_LPC = Over(orderUpsReceive_final.scan_lifetime_page_count, 'lag', 0, 1, 1,0L);
     LAG_COLOR_COPY_LPC = Over(orderUpsReceive_final.color_copy_lifetime_page_count, 'lag', 0, 1, 1,0L);
     LAG_MONO_COPY_LPC = Over(orderUpsReceive_final.mono_copy_lifetime_page_count, 'lag', 0, 1, 1,0L);
     LAG_TOTAL_COPY_LPC = Over(orderUpsReceive_final.total_copy_lifetime_page_count, 'lag', 0, 1, 1,0L);
     LAG_EXTENDS = Over(orderUpsReceive_final.reported_date, 'lag', 0, 1, 1, true);
     stitchUpsReceive = Stitch(orderUpsReceive_final, LAG_RPT_DATE, LAG_LIFE_PAGE_COUNT, LAG_COLOR_LPC, LAG_MONO_LPC, LAG_FAX_LPC, LAG_SCAN_LPC, LAG_COLOR_COPY_LPC, 
     LAG_MONO_COPY_LPC, LAG_TOTAL_COPY_LPC, LAG_EXTENDS);

     
     generate flatten(stitchUpsReceive) AS(                                            account_code,serial_number,reported_date,lifetime_page_count,color_lifetime_page_count,mono_lifetime_page_count,fax_lifetime_page_count,scan_lifetime_page_count,color_copy_lifetime_page_count,mono_copy_lifetime_page_count,total_copy_lifetime_page_count,page_count,color_page_count,mono_page_count,fax_page_count,scan_page_count,color_copy_page_count,mono_copy_page_count,total_copy_page_count,days_since_last_reported,printer_name,ip_address,suspect_state,data_source,time_dimension_surrogate_key,customer_master_surrogate_key,product_master_surrogate_key,material_master_surrogate_key,account_surrogate_key: chararray,asset_surrogate_key,agreement_surrogate_key,create_date_time,create_user,update_date_time,update_user,oldRecord,
                                             LAG_RPT_DATE,
                                             LAG_LIFE_PAGE_COUNT,
                                             LAG_COLOR_LPC,
                                             LAG_MONO_LPC,
                                             LAG_FAX_LPC,
                                             LAG_SCAN_LPC,
                                             LAG_COLOR_COPY_LPC,
                                             LAG_MONO_COPY_LPC,
                                             LAG_TOTAL_COPY_LPC,
                                             LAG_EXTENDS);
    }

lagUpsReceive_gen = foreach lagUpsReceive_final generate
                                             account_code, 
				   	     serial_number,   
                            		     reported_date,
                                             lifetime_page_count,  
                                             color_lifetime_page_count, 
                                             mono_lifetime_page_count, 
                                             fax_lifetime_page_count, 
                                             scan_lifetime_page_count, 
                                             color_copy_lifetime_page_count, 
                                             mono_copy_lifetime_page_count, 
                                             total_copy_lifetime_page_count, 
                                             days_since_last_reported,
                                             printer_name, 
                                             ip_address, 
                                             suspect_state, 
                                             data_source,
					     oldRecord,
                                             time_dimension_surrogate_key,
                                             customer_master_surrogate_key,
                                             product_master_surrogate_key,
                                             material_master_surrogate_key,
                                             account_surrogate_key,
                                             asset_surrogate_key,
                                             agreement_surrogate_key,
                                             LAG_RPT_DATE,
                                             LAG_LIFE_PAGE_COUNT as LAG_LIFE_PAGE_COUNT,
                                             LAG_COLOR_LPC as LAG_COLOR_LPC,
                                             LAG_MONO_LPC as LAG_MONO_LPC,
                                             LAG_FAX_LPC as LAG_FAX_LPC,
                                             LAG_SCAN_LPC as LAG_SCAN_LPC,
                                             LAG_COLOR_COPY_LPC as LAG_COLOR_COPY_LPC,
                                             LAG_MONO_COPY_LPC as LAG_MONO_COPY_LPC,
                                             LAG_TOTAL_COPY_LPC as LAG_TOTAL_COPY_LPC,
                                             LAG_EXTENDS;
describe lagUpsReceive_gen;
--dump lagUpsReceive_gen;

---------------------------------------------Filter out the old records, hmmmmmmmmmm, maybe I should do this later---------------------
--------------If I filter out the old records, how will I get the days_since_last_reported???????????????????????? Oh, I have both dates so it's OK.

lagUpsReceiveFILTER_Final = FILTER lagUpsReceive_gen by EqualsIgnoreCase('$INITIAL', true) OR NOT EqualsIgnoreCase(oldRecord, true) ;

--dump lagUpsReceiveFILTER_Final;

------------------------------------------------------ Convert to ISO dates ----------------------------------------
--Because LAG_RPT_DT has no datatype, it throws an error with ToDate() function.  Must convert it ToString() first.

F = FOREACH lagUpsReceiveFILTER_Final GENERATE account_code, 
				   	     serial_number,   
                            		     ToDate(reported_date, 'yyyy-MM-dd HH:mm:ss') as reported_date,
                                             lifetime_page_count,  
                                             color_lifetime_page_count, 
                                             mono_lifetime_page_count, 
                                             fax_lifetime_page_count, 
                                             scan_lifetime_page_count, 
                                             color_copy_lifetime_page_count, 
                                             mono_copy_lifetime_page_count, 
                                             total_copy_lifetime_page_count, 
                                             days_since_last_reported,
                                             printer_name, 
                                             ip_address, 
                                             suspect_state, 
                                             data_source,
					     oldRecord,
                                             time_dimension_surrogate_key,
                                             customer_master_surrogate_key,
                                             product_master_surrogate_key,
                                             material_master_surrogate_key,
                                             account_surrogate_key,
                                             asset_surrogate_key,
                                             agreement_surrogate_key,
                           ToDate(toString(LAG_RPT_DATE, 'UTF-8'),'yyyy-MM-dd HH:mm:ss') as LAG_RPT_DATE,                                             
                                             LAG_LIFE_PAGE_COUNT,
                                             LAG_COLOR_LPC,
                                             LAG_MONO_LPC,
                                             LAG_FAX_LPC,
                                             LAG_SCAN_LPC,
                                             LAG_COLOR_COPY_LPC,
                                             LAG_MONO_COPY_LPC,
                                             LAG_TOTAL_COPY_LPC,
                                             LAG_EXTENDS;
describe F;
--dump F;

------------------------------------------------------ Convert the ISO date to string ----------------------------------------
--G = FOREACH F GENERATE ToString(reported_date) as reported_date, ToString(lag_date) as lag_date..;

G = FOREACH F GENERATE account_code, 
				   	     serial_number,   
                            		     ToString(reported_date, 'yyyy-MM-dd\'T\'HH:mm:ss.SSSZ') as reported_date,
                                             lifetime_page_count,  
                                             color_lifetime_page_count, 
                                             mono_lifetime_page_count, 
                                             fax_lifetime_page_count, 
                                             scan_lifetime_page_count, 
                                             color_copy_lifetime_page_count, 
                                             mono_copy_lifetime_page_count, 
                                             total_copy_lifetime_page_count,
                                             days_since_last_reported, 
                                             printer_name, 
                                             ip_address, 
                                             suspect_state, 
                                             data_source,
					     oldRecord,
                                             time_dimension_surrogate_key,
                                             customer_master_surrogate_key,
                                             product_master_surrogate_key,
                                             material_master_surrogate_key,
                                             account_surrogate_key,
                                             asset_surrogate_key,
                                             agreement_surrogate_key,
                           ToString(LAG_RPT_DATE, 'yyyy-MM-dd\'T\'HH:mm:ss.SSSZ') as LAG_RPT_DATE,                                             
                                             LAG_LIFE_PAGE_COUNT,
                                             LAG_COLOR_LPC,
                                             LAG_MONO_LPC,
                                             LAG_FAX_LPC,
                                             LAG_SCAN_LPC,
                                             LAG_COLOR_COPY_LPC,
                                             LAG_MONO_COPY_LPC,
                                             LAG_TOTAL_COPY_LPC,
                                             LAG_EXTENDS;

--dump G;

-------------------------------------------------------Remove the timestamp from the date, from the T on ----------------------------
--H = FOREACH G GENERATE SUBSTRING(reported_date, 0, INDEXOF(reported_date, 'T')) as reported_dt, SUBSTRING(lag_date, 0, INDEXOF(lag_date, 'T')) as lag_dt..;

H = FOREACH G GENERATE                       account_code, 
				   	     serial_number,   
                            		     SUBSTRING(reported_date, 0, INDEXOF(reported_date, 'T')) as reported_date,
                                             lifetime_page_count,  
                                             color_lifetime_page_count, 
                                             mono_lifetime_page_count, 
                                             fax_lifetime_page_count, 
                                             scan_lifetime_page_count, 
                                             color_copy_lifetime_page_count, 
                                             mono_copy_lifetime_page_count, 
                                             total_copy_lifetime_page_count, 
                                             days_since_last_reported,
                                             printer_name, 
                                             ip_address, 
                                             suspect_state, 
                                             data_source,
					     oldRecord,
                                             time_dimension_surrogate_key,
                                             customer_master_surrogate_key,
                                             product_master_surrogate_key,
                                             material_master_surrogate_key,
                                             account_surrogate_key,
                                             asset_surrogate_key,
                                             agreement_surrogate_key,
                           SUBSTRING(LAG_RPT_DATE, 0, INDEXOF(LAG_RPT_DATE, 'T')) as LAG_RPT_DATE,                                             
                                             LAG_LIFE_PAGE_COUNT,
                                             LAG_COLOR_LPC,
                                             LAG_MONO_LPC,
                                             LAG_FAX_LPC,
                                             LAG_SCAN_LPC,
                                             LAG_COLOR_COPY_LPC,
                                             LAG_MONO_COPY_LPC,
                                             LAG_TOTAL_COPY_LPC,
                                             LAG_EXTENDS;

describe H;
--dump H;

-------------------------------------------------------Remove the dash from the date, to get yyyymmdd format as required by DiffDate()---------------
--I = FOREACH H GENERATE REPLACE(reported_dt, '-','') as rptDt, REPLACE(lag_dt, '-','') as lagDt..;

I = FOREACH H GENERATE account_code, 
				   	     serial_number,   
                            		     REPLACE(reported_date, '-','') as reported_date,
                                             lifetime_page_count,  
                                             color_lifetime_page_count, 
                                             mono_lifetime_page_count, 
                                             fax_lifetime_page_count, 
                                             scan_lifetime_page_count, 
                                             color_copy_lifetime_page_count, 
                                             mono_copy_lifetime_page_count, 
                                             total_copy_lifetime_page_count, 
                                             days_since_last_reported,
                                             printer_name, 
                                             ip_address, 
                                             suspect_state, 
                                             data_source,
					     oldRecord,
                                             time_dimension_surrogate_key,
                                             customer_master_surrogate_key,
                                             product_master_surrogate_key,
                                             material_master_surrogate_key,
                                             account_surrogate_key,
                                             asset_surrogate_key,
                                             agreement_surrogate_key,
                           REPLACE(LAG_RPT_DATE, '-','') as LAG_RPT_DATE,                                             
                                             LAG_LIFE_PAGE_COUNT,
                                             LAG_COLOR_LPC,
                                             LAG_MONO_LPC,
                                             LAG_FAX_LPC,
                                             LAG_SCAN_LPC,
                                             LAG_COLOR_COPY_LPC,
                                             LAG_MONO_COPY_LPC,
                                             LAG_TOTAL_COPY_LPC,
                                             LAG_EXTENDS;
describe I;
--dump I;  --works OK, even after converting total_copy_lifetime_page_count ToLong() above.

--The original code I pulled this from was converting product_master_surrogate_key ToLong and it caused a "unable to open iterator" error.  It already was a long datatype.

pageCountUpsReceive_load = foreach I {
         CALC_UR_DAYS_BETWEEN = DiffDate(reported_date, LAG_RPT_DATE);
         CALC_UR_PAGE_COUNT_no_value = (lifetime_page_count IS NULL OR LAG_LIFE_PAGE_COUNT IS NULL ? true : false);
 	CALC_COLOR_COUNT_no_value = (color_lifetime_page_count IS NULL OR LAG_COLOR_LPC IS NULL ? true : false);
    	CALC_MONO_COUNT_no_value = (mono_lifetime_page_count IS NULL OR LAG_MONO_LPC IS NULL ? true : false);    
    	CALC_FAX_COUNT_no_value = (fax_lifetime_page_count IS NULL OR LAG_FAX_LPC IS NULL ? true : false);
    	CALC_SCAN_COUNT_no_value = (scan_lifetime_page_count IS NULL OR LAG_SCAN_LPC IS NULL ? true : false);
    	CALC_COLOR_COPY_COUNT_no_value = (color_copy_lifetime_page_count IS NULL OR LAG_COLOR_COPY_LPC IS NULL ? true : false);
    	CALC_MONO_COPY_COUNT_no_value = (mono_copy_lifetime_page_count IS NULL OR LAG_MONO_COPY_LPC IS NULL ? true : false);
    	CALC_TOTAL_COPY_COUNT_no_value = (total_copy_lifetime_page_count IS NULL OR LAG_TOTAL_COPY_LPC IS NULL ? true : false);
         generate
                 account_code,
                              serial_number,   
                              reported_date, 
                              lifetime_page_count,  
                              color_lifetime_page_count, 
                              mono_lifetime_page_count, 
                              fax_lifetime_page_count, 
                              scan_lifetime_page_count, 
                              color_copy_lifetime_page_count, 
                              mono_copy_lifetime_page_count, 
                              total_copy_lifetime_page_count,
                 (CALC_UR_PAGE_COUNT_no_value ? 0 : lifetime_page_count - toLong(LAG_LIFE_PAGE_COUNT)) AS page_count,
		(CALC_COLOR_COUNT_no_value ? 0 : color_lifetime_page_count - toLong(LAG_COLOR_LPC)) as color_page_count,
		(CALC_MONO_COUNT_no_value ? 0 : mono_lifetime_page_count - toLong(LAG_MONO_LPC)) as mono_page_count,
		(CALC_FAX_COUNT_no_value ? 0 : fax_lifetime_page_count - toLong(LAG_FAX_LPC)) as fax_page_count,
		(CALC_SCAN_COUNT_no_value ? 0 : scan_lifetime_page_count - toLong(LAG_SCAN_LPC)) as scan_page_count,
		(CALC_COLOR_COPY_COUNT_no_value ? 0 : color_copy_lifetime_page_count - toLong(LAG_COLOR_COPY_LPC)) as color_copy_page_count,
		(CALC_MONO_COPY_COUNT_no_value ? 0 : mono_copy_lifetime_page_count - toLong(LAG_MONO_COPY_LPC)) as mono_copy_page_count,
		(CALC_TOTAL_COPY_COUNT_no_value ? 0 : total_copy_lifetime_page_count - toLong(LAG_TOTAL_COPY_LPC)) as total_copy_page_count,
/*-----------Following line sets the last reported to 0 IF it has been more than 41 years. This usually is when the printer first reports in-----*/
                 (CALC_UR_DAYS_BETWEEN > 15000 ? 0 : CALC_UR_DAYS_BETWEEN) AS days_since_last_reported,
                                            printer_name, 
                                            ip_address, 
                                            suspect_state, 
                                            data_source,
                                            time_dimension_surrogate_key,
                                            customer_master_surrogate_key,
                                            product_master_surrogate_key,
                                            material_master_surrogate_key,
                                            account_surrogate_key,
                                            asset_surrogate_key,
                                            agreement_surrogate_key,
                                            ToString(CurrentTime(), 'yyyy-MM-dd HH:mm:ss') AS create_date_time,
                                            'Peggy' AS create_user,
                                            ToString(CurrentTime(), 'yyyy-MM-dd HH:mm:ss') AS update_date_time,
                                            'none' AS update_user;
}
describe pageCountUpsReceive_load;
--dump pageCountUpsReceive_load;
STORE pageCountUpsReceive_load into 'DPC_All1' USING PigStorage(',');



