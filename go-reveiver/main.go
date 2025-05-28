package main

import (
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/ipc"
	"github.com/gin-gonic/gin"
)

func main() {
	r := gin.Default()
	r.GET("/", func(c *gin.Context) {
		// 在 handler 函数中，使用 c.JSON 发送一个 JSON 响应
		// http.StatusOK 是标准库定义的 200 状态码
		c.JSON(http.StatusOK, gin.H{
			"status":  "ok",
			"message": "Go Gin app is running!",
		})
	})
	// Define the endpoint to receive Arrow data via POST
	r.POST("/receive-arrow", handleArrowData)
	// Run the server on port 8080
	port := 8080
	fmt.Printf("Go server listening on port %d...\n", port)
	err := r.Run(fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("Failed to run server: %v", err)
	}
}

// 处理单个列的数据
func processColumn(col arrow.Array, field arrow.Field, rowCount int64) {
	fmt.Printf("  Column '%s' (%s):\n", field.Name, field.Type.String())

	switch arr := col.(type) {
	case *array.Int32:
		printInt32Column(arr, rowCount)
	case *array.String:
		printStringColumn(arr, rowCount)
	case *array.Boolean:
		printBooleanColumn(arr, rowCount)
	case *array.Float64:
		printFloat64Column(arr, rowCount)
	default:
		fmt.Printf("    Unsupported column type for printing: %T\n", arr)
	}
	fmt.Println("-----")
}

// 打印 Int32 类型的列数据
func printInt32Column(arr *array.Int32, rowCount int64) {
	for j := 0; j < int(rowCount); j++ {
		if arr.IsNull(j) {
			fmt.Printf("    Row %d: NULL\n", j)
		} else {
			fmt.Printf("    Row %d: %d\n", j, arr.Value(j))
		}
	}
}

// 打印 String 类型的列数据
func printStringColumn(arr *array.String, rowCount int64) {
	for j := 0; j < int(rowCount); j++ {
		if arr.IsNull(j) {
			fmt.Printf("    Row %d: NULL\n", j)
		} else {
			fmt.Printf("    Row %d: %s\n", j, arr.Value(j))
		}
	}
}

// 打印 Boolean 类型的列数据
func printBooleanColumn(arr *array.Boolean, rowCount int64) {
	for j := 0; j < int(rowCount); j++ {
		if arr.IsNull(j) {
			fmt.Printf("    Row %d: NULL\n", j)
		} else {
			fmt.Printf("    Row %d: %t\n", j, arr.Value(j))
		}
	}
}

// 打印 Float64 类型的列数据
func printFloat64Column(arr *array.Float64, rowCount int64) {
	for j := 0; j < int(rowCount); j++ {
		if arr.IsNull(j) {
			fmt.Printf("    Row %d: NULL\n", j)
		} else {
			fmt.Printf("    Row %d: %f\n", j, arr.Value(j))
		}
	}
}

// processRecordBatch 处理一个记录批次
func processRecordBatch(record arrow.Record) {
	log.Printf("Received Record Batch with %d rows and %d columns", record.NumRows(), record.NumCols())

	for i, field := range record.Schema().Fields() {
		col := record.Column(i)
		processColumn(col, field, record.NumRows())
	}
}

// Gin handler function to process incoming Arrow data
func handleArrowData(c *gin.Context) {
	log.Println("Received POST request to /receive-arrow")

	// 读取和准备数据
	reader, err := prepareArrowReader(c)
	if err != nil {
		handleError(c, err)
		return
	}
	defer reader.Release()
	defer c.Request.Body.Close() // 在这里关闭请求体

	log.Printf("Arrow Schema received: %+v", reader.Schema())

	// 处理记录批次
	for reader.Next() {
		record := reader.Record()
		defer record.Release()
		processRecordBatch(record)
	}

	// 检查处理过程中的错误
	if err := reader.Err(); err != nil && err != io.EOF {
		handleError(c, fmt.Errorf("error reading Arrow IPC stream: %v", err))
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Arrow data received and parsed successfully!"})
}

// prepareArrowReader 创建并返回一个 Arrow IPC reader
func prepareArrowReader(c *gin.Context) (*ipc.Reader, error) {
	body := c.Request.Body
	// 注意：不要在这里关闭 body，让调用者来处理关闭
	reader, err := ipc.NewReader(body)
	if err != nil {
		body.Close() // 只在发生错误时关闭
		return nil, fmt.Errorf("failed to create Arrow IPC reader: %v", err)
	}
	return reader, nil
}

// handleError 统一处理错误响应
func handleError(c *gin.Context, err error) {
	log.Printf("Error: %v", err)
	c.JSON(http.StatusInternalServerError, gin.H{"message": err.Error()})
}
