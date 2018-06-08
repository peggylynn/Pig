--Pull any pattern where the last LTPC is less than the max LTPC.  Could cover multiple patterns.

--Please put it at the beginning of the pig script. This is needed for gzip.

-- set compression
--set output.compression.enabled true;
--set output.compression.codec org.apache.hadoop.io.compress.GzipCodec;

--1:  Load the raw data from a file
raw_sn = load 'pattern1_sn_07282016.csv' using PigStorage(',') as 
(account_code:chararray,
serial_number:chararray,
reported_date:chararray,
lifetime_page_count:int,
color_lifetime_page_count: int,
mono_lifetime_page_count: int, 
fax_lifetime_page_count: int, 
scan_lifetime_page_count: int,
color_copy_lifetime_page_count:int,
mono_copy_lifetime_page_count:int,
total_copy_lifetime_page_count:int,
page_count:int,
color_page_count: int, 
mono_page_count: int, 
fax_page_count: int, 
scan_page_count: int, 
color_copy_page_count: int, 
mono_copy_page_count: int, 
total_copy_page_count: int, 
days_since_last_reported: int,
printer_name: chararray,
ip_address: chararray,
suspect_state: int,
data_source: int,
time_dimension_surrogate_key: chararray, 
customer_master_surrogate_key:chararray,
product_master_surrogate_key: chararray, 
material_master_surrogate_key: chararray, 
account_surrogate_key: chararray, 
asset_surrogate_key: chararray,
agreement_surrogate_key: chararray,
create_date_time: chararray,
create_user: chararray,
update_date_time: chararray,
update_user: chararray);

--2:  Get just the desired fields
raw_sn1 = FOREACH raw_sn GENERATE account_code,serial_number,reported_date,ip_address,lifetime_page_count, color_lifetime_page_count,mono_lifetime_page_count,page_count,color_page_count,mono_page_count,
time_dimension_surrogate_key, customer_master_surrogate_key,product_master_surrogate_key,material_master_surrogate_key,
account_surrogate_key,asset_surrogate_key,agreement_surrogate_key,create_date_time,
create_user;

describe raw_sn1;

--3:  Get the affected serial number, that is, any with a negative page count; could be multiple negative page counts.
affected_sn = filter raw_sn1 by page_count < 0;
--dump affected_sn;
describe affected_sn;

affected_sn1 = FOREACH affected_sn GENERATE account_code,serial_number,reported_date as neg_read_date, ip_address,lifetime_page_count as LTPC_Y, color_lifetime_page_count,mono_lifetime_page_count,page_count,color_page_count,mono_page_count,
time_dimension_surrogate_key, customer_master_surrogate_key,product_master_surrogate_key,material_master_surrogate_key,
account_surrogate_key,asset_surrogate_key,agreement_surrogate_key,create_date_time,
create_user;
describe affected_sn1;
--dump affected_sn1;

--4:  Group the affected serial numbers and get a count.
group_by_sn = GROUP affected_sn by (serial_number, ip_address);
describe group_by_sn;
dump group_by_sn;

group_by_sn_flat = FOREACH group_by_sn GENERATE flatten(group) as (serial_number, ip_address), COUNT(affected_sn.account_code) as cnt;
describe group_by_sn_flat;
dump group_by_sn_flat;

/*--We won't do this in this script.  As long as the last LTPC < max LTPC, we want it./*
--5:  Save only the SN with 1 negative page count.

sn_one_neg_read = filter group_by_sn_flat by cnt==1; 
describe sn_one_neg_read; 
--sn_one_neg_read: {serial_number: chararray,cnt: long}
dump sn_one_neg_read;
--STORE sn_one_neg_read into 's3://dev-lxk-home/users/bishopp/LTPC-reset-down-out/sn_one_neg_read' using JsonStorage;
--STORE sn_one_neg_read into 'sn_one_neg_read' using PigStorage(',');

*/
--Rename the relation so the rest of the script makes sense.  Just remember, there could be multiple negative page counts.
sn_one_neg_read = group_by_sn_flat;

--6:  Find the last ltpc per serial number and the maximum ltpc.  If they are different, then the serial number falls into pattern1.  This needs a couple of routines to get this.

--6a: First join sn_one_neg_read back to affected_sn1 to get affected serial numbers and all their data.
affected_sn_one_neg_read = JOIN sn_one_neg_read by serial_number, raw_sn1 by serial_number;

describe affected_sn_one_neg_read;
--dump affected_sn_one_neg_read;
--This should be thousands of rows.......TRUE..........


--GIVES ERRORS:
--Clean out the bag names from affected_sn_one_neg_read.  The idea was to get away from not knowing where fields come from.
--affected_sn_one_neg_read1 = FOREACH affected_sn_one_neg_read GENERATE sn_one_neg_read::serial_number as serial_number, 
affected_sn_one_neg_read1 = FOREACH affected_sn_one_neg_read GENERATE group_by_sn_flat::serial_number as serial_number, 
raw_sn1::account_code as account_code,
raw_sn1::reported_date  as reported_date,
raw_sn1::ip_address  as ip_address,
raw_sn1::lifetime_page_count as lifetime_page_count,
raw_sn1::color_lifetime_page_count as color_lifetime_page_count,
raw_sn1::mono_lifetime_page_count as mono_lifetime_page_count,
raw_sn1::page_count as page_count,
raw_sn1::color_page_count as color_page_count,
raw_sn1::mono_page_count as mono_page_count,
raw_sn1::time_dimension_surrogate_key as time_dimension_surrogate_key,
raw_sn1::customer_master_surrogate_key as customer_master_surrogate_key,
raw_sn1::product_master_surrogate_key as product_master_surrogate_key,
raw_sn1::material_master_surrogate_key as material_master_surrogate_key,
raw_sn1::account_surrogate_key as account_surrogate_key,
raw_sn1::asset_surrogate_key as asset_surrogate_key,
raw_sn1::agreement_surrogate_key as agreement_surrogate_key,
raw_sn1::create_date_time as create_date_time,
raw_sn1::create_user as create_user;
--reported_date as neg_read_date, --NO, this is wrong.  It's all the data so you can't say it's neg_read_date

describe affected_sn_one_neg_read1;
--dump affected_sn_one_neg_read1; 
--This should be thousands of rows. IT IS.  Because it's ALL THE DATA for AFFECTED_SN with ONE NEGATIVE PAGE COUNT.

--6a: Find the max ltpc of the affected serial numbers with only 1 negative page count.  Save for use later in this routine.
--First filter out all data that happened on or after the negative page count date.
--Get the negative page count date. It's in affected_sn1.
affected_sn1_data = JOIN affected_sn_one_neg_read1 by serial_number, affected_sn1 by serial_number;
describe affected_sn1_data;
--dump affected_sn1_data; --Works

pre_ltpc_y_data = FILTER affected_sn1_data by reported_date < neg_read_date;
describe pre_ltpc_y_data;
--dump pre_ltpc_y_data; --Works, about 8000 rows
--STORE pre_ltpc_y_data into 'pre_ltpc_y_data';

pre_ltpc_y_data1 = FOREACH pre_ltpc_y_data GENERATE 
affected_sn_one_neg_read1::serial_number as serial_number,
affected_sn_one_neg_read1::account_code as account_code,
affected_sn_one_neg_read1::reported_date as reported_date,
affected_sn_one_neg_read1::ip_address as ip_address,
affected_sn_one_neg_read1::lifetime_page_count as lifetime_page_count,
affected_sn1::neg_read_date as neg_read_date,
affected_sn1::LTPC_Y as LTPC_Y,
affected_sn_one_neg_read1::color_lifetime_page_count as color_lifetime_page_count,
affected_sn_one_neg_read1::mono_lifetime_page_count as mono_lifetime_page_count,
affected_sn_one_neg_read1::page_count as page_count,
affected_sn_one_neg_read1::color_page_count as color_page_count,
affected_sn_one_neg_read1::mono_page_count as mono_page_count,
affected_sn_one_neg_read1::time_dimension_surrogate_key as time_dimension_surrogate_key,
affected_sn_one_neg_read1::customer_master_surrogate_key as customer_master_surrogate_key,
affected_sn_one_neg_read1::product_master_surrogate_key as product_master_surrogate_key,
affected_sn_one_neg_read1::material_master_surrogate_key as material_master_surrogate_key,
affected_sn_one_neg_read1::account_surrogate_key as account_surrogate_key,
affected_sn_one_neg_read1::asset_surrogate_key as asset_surrogate_key,
affected_sn_one_neg_read1::agreement_surrogate_key as agreement_surrogate_key,
affected_sn_one_neg_read1::create_date_time as create_date_time,
affected_sn_one_neg_read1::create_user as create_user;
describe pre_ltpc_y_data1;
--dump pre_ltpc_y_data1; --Works. For each row, the neg_page_count and neg_read_date is in there.  It is the same for each row

--group_for_ltpc_x = GROUP affected_sn_one_neg_read1 by serial_number;
--Added in to group also by ip_address on Jul 26, 2016, since serial number is not unique.  It's more likely to be unique if we consider also the ip_address.
group_for_ltpc_x_y = GROUP pre_ltpc_y_data1 by (serial_number,ip_address);
--Yields a shorter schema than group_for_ltpc_xa
describe group_for_ltpc_x_y;
--dump group_for_ltpc_x; --Works

ltpc_x_y = FOREACH group_for_ltpc_x_y GENERATE MIN(pre_ltpc_y_data1.serial_number) as serial_number, 
MIN(pre_ltpc_y_data1.ip_address) as ip_address,
MAX(pre_ltpc_y_data1.lifetime_page_count) as ltpc_x,
MAX(pre_ltpc_y_data1.reported_date) as ltpc_x_date,
MAX(pre_ltpc_y_data1.neg_read_date) as neg_read_date,
MAX(pre_ltpc_y_data1.LTPC_Y) as LTPC_Y;
describe ltpc_x_y;
dump ltpc_x_y; --Expect one row per affected SN. TRUE
--STORE ltpc_x_y into 's3://dev-lxk-home/users/bishopp/LTPC-reset-down-out/ltpc_x_y' using JsonStorage;
--STORE ltpc_x_y into 'ltpc_x_y' using PigStorage(',');
--The data is CORRECT now that the date is in YYYY-MM-DD ----SEE IF YOU CAN INCLUDE ltpc_y

--Jul 27, 2016: I'm not sure what this relation is for because I transform group_by_sn to group_by_sn_flat above and then rename it to sn_one_neg_read (even though it's not just SN with on neg read). YES, THIS APPEARS UNUSED.
/*
group_for_ltpc_x_flat = FOREACH group_by_sn GENERATE flatten(group) as (serial_number, ip_address), 
pre_ltpc_y_data1.account_code as account_code,
pre_ltpc_y_data1.reported_date as reported_date,
pre_ltpc_y_data1.ip_address as ip_address,
pre_ltpc_y_data1.neg_read_date as neg_read_date,
pre_ltpc_y_data1.LTPC_Y as LTPC_Y,
pre_ltpc_y_data1.lifetime_page_count as lifetime_page_count,
pre_ltpc_y_data1.color_lifetime_page_count as color_lifetime_page_count,
pre_ltpc_y_data1.mono_lifetime_page_count as mono_lifetime_page_count,
pre_ltpc_y_data1.page_count as page_count,
pre_ltpc_y_data1.color_page_count as color_page_count,
pre_ltpc_y_data1.mono_page_count as mono_page_count,
pre_ltpc_y_data1.time_dimension_surrogate_key as time_dimension_surrogate_key,
pre_ltpc_y_data1.customer_master_surrogate_key as customer_master_surrogate_key,
pre_ltpc_y_data1.product_master_surrogate_key as product_master_surrogate_key,
pre_ltpc_y_data1.material_master_surrogate_key as material_master_surrogate_key,
pre_ltpc_y_data1.account_surrogate_key as account_surrogate_key,
pre_ltpc_y_data1.asset_surrogate_key as asset_surrogate_key,
pre_ltpc_y_data1.agreement_surrogate_key as agreement_surrogate_key,
pre_ltpc_y_data1.create_date_time as create_date_time,
pre_ltpc_y_data1.create_user as create_user;

describe group_for_ltpc_x_flat; 
--dump group_for_ltpc_x_flat; --Error: unable to open iterator
*/

--6b: Get the serial number with the last ltpc: 
   --1: Get the max reported_date from #6a.
   --1a: First group the data
affected_sn_one_neg_read1_grp = GROUP affected_sn_one_neg_read1 by (serial_number, ip_address);
describe affected_sn_one_neg_read1_grp;
dump affected_sn_one_neg_read1_grp; --This gives all data for each serial number by serial number; I THINK it's ok....
--STORE affected_sn_one_neg_read1_grp into 's3://dev-lxk-home/users/bishopp/LTPC-reset-down-out/affected_sn_one_neg_read1_grp' using JsonStorage;
--STORE affected_sn_one_neg_read1_grp into 'affected_sn_one_neg_read1_grp' using PigStorage(',';

ltpc_z_date = FOREACH affected_sn_one_neg_read1_grp GENERATE MIN(affected_sn_one_neg_read1.serial_number) as serial_number, MIN(affected_sn_one_neg_read1.ip_address) as ip_address, MAX(affected_sn_one_neg_read1.reported_date) as ltpc_z_date;
describe ltpc_z_date;
dump ltpc_z_date; --Works and looks good.
--STORE ltpc_z_date into 's3://dev-lxk-home/users/bishopp/LTPC-reset-down-out/ltpc_z_date' using JsonStorage;
--STORE ltpc_z_date into 'ltpc_z_date' using PigStorage(',');
   --1b: Now join to the data to get the ltpc_z from that date

ltpc_z = JOIN ltpc_z_date by (serial_number,ltpc_z_date), raw_sn1 by (serial_number,reported_date);
describe ltpc_z;
dump ltpc_z; --Works, yes it still works. 8 rows - one for each SN
--STORE ltpc_z into 's3://dev-lxk-home/users/bishopp/LTPC-reset-down-out/ltpc_z' using JsonStorage;

ltpc_z1 = FOREACH ltpc_z GENERATE 
raw_sn1::serial_number as serial_number,
raw_sn1::account_code as account_code,
raw_sn1::reported_date as ltpc_z_date,
raw_sn1::ip_address as ip_address,
raw_sn1::lifetime_page_count as LTPC_Z,
raw_sn1::color_lifetime_page_count as color_lifetime_page_count,
raw_sn1::mono_lifetime_page_count as mono_lifetime_page_count,
raw_sn1::page_count as page_count,
raw_sn1::color_page_count as color_page_count,
raw_sn1::mono_page_count as mono_page_count,
raw_sn1::time_dimension_surrogate_key as time_dimension_surrogate_key,
raw_sn1::customer_master_surrogate_key as customer_master_surrogate_key,
raw_sn1::product_master_surrogate_key as product_master_surrogate_key,
raw_sn1::material_master_surrogate_key as material_master_surrogate_key,
raw_sn1::account_surrogate_key as account_surrogate_key,
raw_sn1::asset_surrogate_key as asset_surrogate_key,
raw_sn1::agreement_surrogate_key as agreement_surrogate_key,
raw_sn1::create_date_time as create_date_time,
raw_sn1::create_user as create_user;
describe ltpc_z1; --OK
dump ltpc_z1;--Error: cannot open iterator for alias ltpc_z1. This error got fixed when I changed the qualifier from "." to "::"!!!!!
--STORE ltpc_z 1into 's3://dev-lxk-home/users/bishopp/LTPC-reset-down-out/ltpc_z1' using JsonStorage;

--6c: Get the serial number with the ltpc_x, ltpc_x_date, ltpc_y, ltpc_y_date, ltpc_z, ltpc_z_date and all the other fields:
pattern1_sn = Join ltpc_x_y by serial_number, ltpc_z1 by serial_number;
describe pattern1_sn;
dump pattern1_sn;
--STORE pattern1_sn into 'pattern1_sn';

--6cc: The dump in 6d always throws an error.  Clean up pattern1_sn first then try 6d.  Geeze.
pattern1_sn_ren = FOREACH pattern1_sn GENERATE 
ltpc_x_y::serial_number as serial_number,
ltpc_x_y::ip_address as ip_address,
ltpc_x_y::ltpc_x as ltpc_x,
ltpc_x_y::ltpc_x_date as ltpc_x_date,
ltpc_x_y::neg_read_date as neg_read_date,
ltpc_x_y::LTPC_Y as LTPC_Y,
ltpc_z1::ltpc_z_date as ltpc_z_date,
ltpc_z1::LTPC_Z as LTPC_Z,
ltpc_z1::account_code as account_code,
ltpc_z1::color_lifetime_page_count as color_lifetime_page_count,
ltpc_z1::mono_lifetime_page_count as mono_lifetime_page_count,
ltpc_z1::page_count as page_count,
ltpc_z1::color_page_count as color_page_count,
ltpc_z1::mono_page_count as mono_page_count,
ltpc_z1::time_dimension_surrogate_key as time_dimension_surrogate_key,
ltpc_z1::customer_master_surrogate_key as customer_master_surrogate_key,
ltpc_z1::product_master_surrogate_key as product_master_surrogate_key,
ltpc_z1::material_master_surrogate_key as material_master_surrogate_key,
ltpc_z1::account_surrogate_key as account_surrogate_key,
ltpc_z1::asset_surrogate_key as asset_surrogate_key,
ltpc_z1::agreement_surrogate_key as agreement_surrogate_key,
ltpc_z1::create_date_time as create_date_time,
ltpc_z1::create_user as create_user;
describe pattern1_sn_ren;
--dump pattern1_sn_ren;

--6d: using the two sets above, ltpc_z and ltpc_x, filter where ltpc_x > ltpc_z.  This is Pattern1 serial numbers with the crucial LTPC values 
--ssto calculate pages not billed.
--pattern1a = FILTER pattern1_sn by ltpc_x_y.ltpc_x > ltpc_z1.LTPC_Z; 
--AT the DUMP:Unable to open iterator for alias pattern1a.  I get this with > or <.
--THE FIX:  clean up the column qualifiers first!
pattern1 = FILTER pattern1_sn_ren by ltpc_x > LTPC_Z;
--pattern1a = FILTER pattern1_sn_ren by ltpc_x > 900000;
describe pattern1;
--dump pattern1;  --Now this works with either "pattern1 = FILTER pattern1_sn by ltpc_x > LTPC_Z;" or "pattern1 = FILTER pattern1_sn_ren by ltpc_x > LTPC_Z;"  
--Perhaps this has no data. NO, THE PROBLEM WAS WITH THE INHERITANCE OF THE COLUMNS FROM THE RELATIONS.  YOU HAVE TO RENAME BEFORE FILTERRING.
--Still get the same error with a hard-coded value.

STORE pattern1 into 'pattern07282016' using PigStorage(',');
--Jul 27, 2016:  success, but it looks like there are dups.

--STORE pattern1 into '$Output' using PigStorage();  --Need stored as tab-delimited
--STORE pattern1 into 's3://dev-lxk-home/users/bishopp/LTPC-reset-down-out/ltpc_final' using JsonStorage;




