STEP 1 :
	Import the folder into Eclipse
STEP 2 :
	Run Davisbase.java
STEP 3 :

	Run following SQL commands to check davisbase functionalities :

 
 	SELECT * FROM table_name; 
 
	SELECT * FROM table_name WHERE rowid = <value>; 
 
	INSERT INTO table_name values (<value1>,<value2>.....);
 
 	CREATE TABLE table_name (row_id INT PRIMARY KEY, variable INT [NOT NULL , 		DEFAULT , AUTOINCREMENT]); -------> Use only either of the constraints and enter 	null to check for them
	* Example * create table table1 ( row_id int , salary int not null );
 
 	
	UPDATE table_name SET column_namne =value WHERE row_id = value;  
 
	DELETE FROM TABLE table_name where row_id = value; 
 
 	DROP TABLE table_name;

	help ; ------> To get information on the commands 


-----------