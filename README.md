# pig
Pig scripts

The original problem here was that the business was complaining about getting negative page counts on customer printers.  This prevents billing because billing is done on the number of pages printed per billing cycle.  The number of pages printed is calculated by subtracting last bill's lifetime page count (LTPC) from this cycle's LTPC.  If the LTPC goes to zero, then the difference is negative.

The suspected root cause was that the printers that exhibited this behavior had undergone service.  Since the lifetime page count is stored in software, this is possible and has been witnessed.  But was it the reason for every time the LTPC resets down? We needed to find out.

My first step was to profile the data to see what the patterns were leading up to the time that the differential page count occurred.  I initially found 8 patterns then later a ninth.

Because Lexmark can bill only when the LTPC this cycle is greater than last cycle, we decided to focus on the patterns where the LTPC drops to zero and has not yet recovered to the former maximum value. (This situation also meant customers were getting a lot of free printed pages). This was patterns 1 & 2 as seen in the image file, "LTPC Patterns with Negative Page Counts.png".

Though our design philosophy for the data warehouse was not to hide data problems that need to be fixed at the source, business pressure overruled us and we were forced to split up the fact table that stored the LTPC and differential counts so that no negative values appeared in reports.  To accompany that, any data that exhibited a negative differential was saved in a separate exception table.

Because the Pig script calculates the differential page count and the days in between, removing the exceptions and storing them into a separate table means that the resulting differentials are not the same as if we kept all the data in one fact table.  Because the LTPC can remain lower than the previous max LTPC, the days between could add up to months or years between "good" readings.  In order to better profile a large number of records, merely joining the fact table and its corresponding exception table did not yield correct page count differentials and days between.  The volume of data was too great to run these calculations on the fly in a BI tool.

I set out to create a table with all the data, but instead of completely copying the original logic from the source files (and replicating logic done in the production pig script), I had one of our ETL developers spin off an intermediate file with all the data partly transformed from the production Pig script that creates the files for the fact table and exception table.

This Pig script, dpc_all.pig, takes that intermediate file and transforms it into the file that is copied to Redshift.  The table was called Device_Page_Count_All.  From this Redshift table I was able to create any BI reports needed.

The LTPC-Reset-Down.pig script is a targeted script to answer specific business questions on the negative page counts.  Because Device_Page_Count_All table contained all patterns, it was more efficient for the BI tool to have a table for just patterns 1 & 2.  That's what LTPC-Reset-Down.pig was for.
