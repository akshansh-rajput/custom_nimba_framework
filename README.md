# custom_nimba_framework
Framework to execute transformation on data using custom build mapper and reducer
## Architecture
This framework consist of Task Controller, Mapper, Reducer and few other internal services.
### Components Overview
1. **Task Controller**

    i. **Responsibility:** The Task Controller is responsible for managing the overall execution of mapper and reducer tasks. It takes user input to determine the number of mappers and reducers required for the job.<br>
    ii. **Dynamic Data Allocation:** It dynamically allocates data, ensuring efficient utilization of computing power.<br>
    iii. **Fault Tolerance:** In case of failures, it not only restarts the failed mappers or reducers but also retains the processed data, avoiding reprocessing of the entire dataset.

2. **Mapper**

    i. **Initial Data Processing:** The Mapper component performs the initial data processing step. It reads data from the source and applies transformations.<br>
    ii. **Isolated Processing:** Each Mapper operates in isolation, processing its own data without awareness of other parallel mappers. The Task Controller manages the coordination of multiple mappers.<br>
    iii. **Data Storage:** Processed data is stored in JSON format in key-value pairs, making it ready for subsequent processing.
   
3. **Reducer**

    i. **Final Data Aggregation:** The Reducer component runs after the Mapper phase and is responsible for finalizing the operation by combining the transformations performed by mappers.<br>
    ii. **Key-Based Processing:** Reducers process unique keys to ensure proper data aggregation. No two reducers work on the same key, enhancing parallelism.<br>
    iii. **Output Formats:** Reducers can produce output in either JSON or CSV format, providing flexibility in result presentation. If CSV is selected as an output format then before writing data it will create super impose schema and then use that schema column as an header.<br>
<br><br>
This architecture ensures efficient and fault-tolerant data processing, with each component playing a specific role in the overall data transformation and aggregation process.
![Blank document](https://github.com/akshansh-rajput/custom_nimble_framework/assets/68275056/10254463-7981-4268-838b-a3a3be2f9455)

## Language used and prerequisite:
1. Scala : 2.12.3
2. Java  : 11.0.1
3. Maven
## Steps to run this framework on local
1. Navigate inside the dir `custom_nimba_framework`.
2. create data dir inside `custom_nimba_framework`
   ```
   mkdir data
   cd data
   mkdir clicks
   mkdir users
   ```
3. Update clicks files in clicks dir and users files in users dir.
4. Navigate back to `custom_nimba_framework` dir and run maven command to build package
    ```
   mvn clean package
    ```
5. Execute below command to start the job.
   ```
   java -cp target/nimba-framework-1.0.0-SNAPSHOT-jar-with-dependencies.jar com.nimba.MainRunner
   ```
<br>After job run successfully, It will create `output` dir and final result will be saved inside this dir.
