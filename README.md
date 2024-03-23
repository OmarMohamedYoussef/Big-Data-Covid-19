
# COVID Dataset Pipeline Workflow

## Description
This project aims to create an automated pipeline workflow from data ingestion to visualization for the COVID dataset. The pipeline includes steps for ingesting the dataset, processing it using HDFS and Hive, executing workflow actions with Oozie, and visualizing the final output using Power BI.

## Business Requirements
1. **Map Visualization**: Show the top 10 ranking countries in death rate on a map.
2. **Map Visualization**: Show the top 10 ranking countries in testing rate on a map.
3. **Pie Chart**: Show the top 10 ranking countries in testing rate on a pie chart.
4. **Custom Chart**: Add a custom chart of choice in the empty section of the dashboard.

### Tool Description
- **Virtualbox**: Software for running virtual machines on your computer.
- **WinSCP**: A popular SFTP and SCP client for Windows that enables secure file transfers between local and remote computers.
- **Cloudera Quickstart**: A pre-configured virtual machine with Hadoop ecosystem components like HDFS, Hive, and Oozie.

#### WinSCP Login
- **IP Address**: Obtain from the terminal using `ifconfig` or `ip add`.
- **Username**: cloudera
- **Password**: cloudera

## Resources
- **Videos**: Links to tutorial videos for assistance with the project.
  - [Virtualbox](https://www.virtualbox.org/)
  - [WinSCP](https://sourceforge.net/projects/winscp/)
  - [Coudera Quickstart](https://www.ic.kmitl.ac.th/moodle/mod/url/view.php?id=2998)
  
## Steps
1. Create a folder named "/home/cloudera/covid_project" on the Virtual Machine.
2. Create subfolders under "covid_project" named "landing_zone" and "scripts".
3. Upload the dataset "covid-19.csv" into the VM using WinSCP to the landing zone folder: "/home/cloudera/covid_project/landing_zone/COVID_SRC_LZ".
4. Load the dataset from "COVID_SRC_LZ" to HDFS directory named "/user/cloudera/ds/COVID_HDFS_LZ" using HDFS CLI commands in a shell script.
5. Create a database on Hive and create a schema for each Hive loading stage:
   - First Hive staging table: Points to the dataset location to select data from.
   - Second Hive ORC table: Partitioned by Country and dynamically loads data into it to speed up queries.
   - Third final Hive table: Generates the final report, which will generate an output file for visualization.
6. Create an Oozie workflow with actions (using Cloudera HUE) to run the HDFS shell script and execute the Hive queries (HDFS and Hive actions).
7. Manually run the Oozie workflow job from the HUE to get the final output.
8. Retrieve the generated final output file from the HDFS file location of the last Hive table: "/user/cloudera/ds/COVID_FINAL_OUTPUT".
9. Download the final output report file and visualize it on Power BI.

## Files
1. **Dataset**:
   - covid-19.csv
   - covid-19 August.xlsx
2. **HDFS Script**: Linux shell script for HDFS data loading.
3. **Hive Code**: HQL scripts for creating Hive tables and processing data.
4. **Oozie**: Sample Oozie script includes workflow, job.properties, and run.sh.

#
HDFS Script with name Load_COVID_TO_HDFS.sh
```sh
#!/bin/bash

#Landing Zones in Linux and HDFS
LINUX_LANDING_AREA=/home/cloudera/covid_project/landing_zone/COVID_SRC_LZ
HDFS_LZ=/user/cloudera/ds/COVID_HDFS_LZ


echo "GLOBAL Variables= $LINUX_LANDING_AREA ", " $HDFS_LZ"


hdfs dfs -mkdir -p $HDFS_LZ
echo "COVID_HDFS_LZ CREATED sucessfully"


hdfs dfs -put $LINUX_LANDING_AREA/covid-19.csv $HDFS_LZ
echo "covid-19.csv dataset LOADED successfully"
```

If the command gives error : 

```sh
   /bin/bash^M: bad interpreter: No such file or directory
```

Use the script : 

```sh
   sed -i -e 's/\r$//' Load_COVID_TO_HDFS.sh
```
#

## Project Steps

1- After running the script and loading the dataset into HDFS, you can make sure in terminal that the data is loaded successfully

```sh
    HDFS DFS -ls -R /user/cloudera
```

2- Open the web browser, open HUE, and select the file, then choose from Query --> Hive

3- Add the SQL command as the file HiveCode.HQL

Note: maybe you can face the error below because of the low memory, so the HiveCode is edited to be suitable to insert the dataset from the covid_staging table into covid_ds_partitioned

```sh
    Error while compiling statement: FAILED: SemanticException org.apache.hadoop.hive.ql.metadata.InvalidTableException: Table not found covid_ds_partitioned
```

4- Open Oozie Workflow and create Workflow to run HDFS file and Hive command as shown below,

![GitHub Logo](https://github.com/OmarMohamedYoussef/Big-Data-Covid-19/blob/main/pic/Oozie.jpg)

Note: there are three files to run Oozie workflow:
    - workflow.xml
    - job.properties
    - run.sh

5- Download the Dataset from the output file.

6- Open Power BI to report file and visualize it.
