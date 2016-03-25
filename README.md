# Persistent cache comparison

## Intro
This is sample reference application to scrutinize two local file storages for use as high-throughput disk-backed cache. 
Candidates are [RocksDB](http://rocksdb.org/) and locally placed one-node version of [Aerospike](http://www.aerospike.com/).
The main idea is to check if we can achieve performance comparable to in-memory data grid.

## Application description
Sample app emulates input stream of gps ticks from many sensors, and aggregating server, merging them together to build trips. 
Server uses persistent cache for real-time incremental trip building.
## Modules
Two modules should be run in separate processes for better measurements.    
### point-data-stream
Generates high-rate gps data stream to socket or kafka topic. 
    
Build and run:        
*cd point-data-stream    
mvn clean package    
java -jar target/point-data-stream-1.0-SNAPSHOT.jar* 
    
Parameters (set as jvm properties *-Dparam.name=value*):        
packet.size - number of events per second (default is 100 which is very small).    
mode - ```<socket|kafka>``` - default is socket, which pushes data to localhost:8099 (note: server process should be listening already).  
trips.day.limit - default is 1M, please do not exceed this value.     
trips.day.multiplier - default is 300, which means each trip is duplicated 300 times (with new id), giving 300M unique trips.    
Change this param with above setting to achieve desired sensors number.       

### trip-generator

Listens to gps data on localhost:8099, creates/updates trips at runtime. Each transaction does at least one cache lookup (create),
or 2 (find and update/complete).    
ForkJoinPool is used as main executor, its queue size is an indicator of desired calculation speed: 
queue size should not grow to infinite values, it is periodically written to output.        
Build and run:    
*cd trip-generator    
mvn clean package    
cd target/bin    
./trip-generator.sh ```<params>```*    

Parameters:    
arg[0] = ```<aerospike|rocksdb>```    

## Results
### Hardware
Aws instance type: r3.2xlarge (8 vCPU, 61Gb RAM), external SSD volume 200Gb, configured according to [aerospike guide](http://www.aerospike.com/docs/deploy_guides/aws/tune/)    
### Numbers observed
RocksDB: 70K tps    
Aerospike: 40K tps    
### Conclusion
With a bit of tuning, RocksDb looks like better alternative. Both storages provide performance comparable to in-memory grids, only level 
of magnitude slower.
