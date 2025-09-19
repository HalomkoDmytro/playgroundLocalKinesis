# Playground local Amazon Kinesis test with SPRING BOOT
REST API endpoints for interacting with the Kinesis service.

### 1. Sets up LocalStack with (only) Kinesis service in docker
* Start docker with Kinesis on port 4566
```docker-compose up -d```
* Test localstack is up
```GET: http://localhost:4566/_localstack/health```
* Check LocalStack logs:
```docker-compose logs localstack```
* Restart LocalStack: 
```docker-compose restart localstack```
* Stop localstack
```docker-compose down```
* To remove all LocalStack data:
```docker-compose down -v```




