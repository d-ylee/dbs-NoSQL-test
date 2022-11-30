# dbs-NoSQL-test
DBS testing with using NoSQL databases

Dennis Lee

dylee@fnal.gov

## Requirements
* Local MongoDB server with port 27017
  * `docker-compose.yml` can start a MongoDB server accessible via localhost at port 27017
  * MongoDB can also be started by running `docker run -it --network dbs-network --name dbs-mongo -p 27017:27017 -d mongo:6.0.2-focal`
  * During testing, these are hard coded into the server
* `curl`
* Dataset
  * `fileLumiData.json` was created from a database dump of the FileLumi table, which was then converted to JSON

## Trying out the code
* `go get` the dependencies
* `go run main.go` to start the server
* Using `upload_fls.sh` upload `fileLumiData.json`
  * This uses `curl`
