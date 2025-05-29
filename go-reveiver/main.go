package main

import (
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/ipc"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/gin-gonic/gin"
)

// 创建一个全局的内存分配器
var allocator *memory.GoAllocator

func init() {
	allocator = memory.NewGoAllocator()
}

func main() {
	r := gin.Default()
	r.GET("/", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"status":  "ok",
			"message": "Go Arrow receiver is running!",
		})
	})
	r.POST("/receive-arrow", handleArrowData)

	port := 8080
	log.Printf("Go Arrow receiver listening on port %d...\n", port)
	if err := r.Run(fmt.Sprintf(":%d", port)); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}

// formatValue 格式化单个值，处理 nil 和字符串格式化
func formatValue(v interface{}) string {
	if v == nil {
		return "NULL"
	}
	switch val := v.(type) {
	case string:
		return fmt.Sprintf("\"%s\"", val)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// handleArrowData 处理接收到的 Arrow 数据
func handleArrowData(c *gin.Context) {
	contentType := c.Request.Header.Get("Content-Type")
	log.Printf("接收到请求 | Content-Type: %s", contentType)

	if contentType != "application/vnd.apache.arrow.stream" {
		handleError(c, fmt.Errorf("不支持的 Content-Type: %s，需要 application/vnd.apache.arrow.stream", contentType))
		return
	}

	// 使用自定义内存分配器创建 reader
	reader, err := ipc.NewReader(c.Request.Body, ipc.WithAllocator(allocator))
	if err != nil {
		handleError(c, fmt.Errorf("创建 Arrow IPC reader 失败: %v", err))
		c.Request.Body.Close()
		return
	}
	defer reader.Release()
	defer c.Request.Body.Close()

	schema := reader.Schema()
	log.Printf("接收到 Arrow Schema:\n%s", schema)

	rowCountTotal := 0
	batchCount := 0

	// 处理所有批次
	for reader.Next() {
		// 创建一个新的函数作用域来处理每个批次
		func() {
			record := reader.Record()
			if record == nil {
				log.Printf("警告：遇到空记录，跳过处理")
				return
			}
			defer record.Release() // 确保在函数返回时释放记录

			if err := processBatch(record, batchCount); err != nil {
				log.Printf("警告：处理批次 %d 时发生错误: %v", batchCount+1, err)
				return
			}

			rowCountTotal += int(record.NumRows())
			batchCount++
		}()

		if err := reader.Err(); err != nil {
			handleError(c, fmt.Errorf("读取批次时发生错误: %v", err))
			return
		}
	}

	log.Printf("成功处理完所有数据 | 总批次数: %d, 总行数: %d", batchCount, rowCountTotal)
	c.JSON(http.StatusOK, gin.H{
		"message": fmt.Sprintf("成功接收并解析 Arrow 数据，共处理 %d 行", rowCountTotal),
		"batches": batchCount,
		"rows":    rowCountTotal,
	})
}

// processBatch 处理单个批次的数据
func processBatch(record arrow.Record, batchNum int) error {
	log.Printf("处理第 %d 批次数据 | 行数: %d, 列数: %d",
		batchNum+1, record.NumRows(), record.NumCols())

	numRows := int(record.NumRows())
	numCols := int(record.NumCols())

	// 检查行列数
	if numRows <= 0 || numCols <= 0 {
		return fmt.Errorf("无效的行数 (%d) 或列数 (%d)", numRows, numCols)
	}

	// 打印数据
	for i := 0; i < numRows; i++ {
		fmt.Printf("\n=== 行 %d ===\n", i+1)
		for j := 0; j < numCols; j++ {
			// 使用函数作用域来处理每个单元格
			func() {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("警告：处理单元格时发生错误: %v", r)
					}
				}()

				col := record.Column(j)
				if col == nil || col.Len() <= i {
					return
				}

				field := record.Schema().Field(j)
				fmt.Printf("%s (%s): ", field.Name, field.Type)
				printArrowValue(col, field, i, 1)
				fmt.Println()
			}()
		}
	}

	return nil
}

// printArrowValue 递归函数，根据类型打印 Arrow Array 在指定行索引处的值
func printArrowValue(arr arrow.Array, field arrow.Field, rowIndex int, indentLevel int) {
	if rowIndex < 0 || rowIndex >= arr.Len() {
		fmt.Print("<Invalid Index>")
		return
	}

	if arr.IsNull(rowIndex) {
		fmt.Print("NULL")
		return
	}

	indent := strings.Repeat("  ", indentLevel)

	switch arr := arr.(type) {
	case *array.Int32:
		fmt.Print(arr.Value(rowIndex))
	case *array.Uint32:
		fmt.Print(arr.Value(rowIndex))
	case *array.Int64:
		fmt.Print(arr.Value(rowIndex))
	case *array.String:
		fmt.Printf("\"%s\"", arr.Value(rowIndex))

	case *array.Struct:
		fmt.Print("{")
		structType := field.Type.(*arrow.StructType)
		fields := structType.Fields()
		for i := 0; i < arr.NumField(); i++ {
			if i > 0 {
				fmt.Print(", ")
			}
			fmt.Printf("\n%s  %s: ", indent, fields[i].Name)
			childArr := arr.Field(i)
			printArrowValue(childArr, fields[i], rowIndex, indentLevel+1)
		}
		if arr.NumField() > 0 {
			fmt.Printf("\n%s", indent)
		}
		fmt.Print("}")

	case *array.List:
		fmt.Print("[")
		if start, end, ok := safeGetOffsets(arr, rowIndex); ok && start <= end {
			listValues := arr.ListValues()
			elemField := field.Type.(*arrow.ListType).ElemField()

			for i := start; i < end && int(i) < listValues.Len(); i++ {
				if i > start {
					fmt.Print(", ")
				}
				printArrowValue(listValues, elemField, int(i), indentLevel+1)
			}
		}
		fmt.Print("]")

	case *array.Map:
		fmt.Print("{")
		if start, end, ok := safeGetOffsets(arr, rowIndex); ok && start <= end {
			mapType := field.Type.(*arrow.MapType)
			keys := arr.Keys()
			items := arr.Items()

			for i := start; i < end; i++ {
				if i > start {
					fmt.Print(", ")
				}
				idx := int(i)
				if idx < keys.Len() && idx < items.Len() {
					fmt.Printf("\n%s  ", indent)
					fmt.Print("key: ")
					printArrowValue(keys, mapType.KeyField(), idx, indentLevel+1)
					fmt.Print(", value: ")
					printArrowValue(items, mapType.ItemField(), idx, indentLevel+1)
				}
			}
			if end > start {
				fmt.Printf("\n%s", indent)
			}
		}
		fmt.Print("}")

	default:
		fmt.Printf("<Unsupported Type: %T>", arr)
	}
}

// safeGetOffsets 安全地获取数组偏移量
func safeGetOffsets(arr arrow.Array, rowIndex int) (start, end int64, ok bool) {
	if rowIndex < 0 || rowIndex >= arr.Len() {
		return 0, 0, false
	}

	switch a := arr.(type) {
	case *array.List:
		offsets := a.Offsets()
		if rowIndex+1 >= len(offsets) {
			return 0, 0, false
		}
		start = int64(offsets[rowIndex])
		end = int64(offsets[rowIndex+1])
		return start, end, start <= end
	case *array.Map:
		offsets := a.Offsets()
		if rowIndex+1 >= len(offsets) {
			return 0, 0, false
		}
		start = int64(offsets[rowIndex])
		end = int64(offsets[rowIndex+1])
		return start, end, start <= end
	default:
		return 0, 0, false
	}
}

// handleError 统一处理错误响应
func handleError(c *gin.Context, err error) {
	log.Printf("错误: %v", err)
	c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
}
