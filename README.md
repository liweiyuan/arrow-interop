### 使用Apache Arrow来在不同语言之间传递数据

#### Java java-sender

- ./mvnmw spring-boot:run

#### Go   go-receiver

- go mod tidy  && go run main.go 


#### call 

- curl http://localhost:9000/send-arrow