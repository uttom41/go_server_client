package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	"github.com/segmentio/kafka-go"
)

var db *gorm.DB
var err error

// Column represents a column in the table
type Column struct {
	Name       string `json:"name"`
	DataType   string `json:"data_type"`
	IsNullable bool   `json:"is_nullable"`
	IsPrimary  bool   `json:"is_primary"`
}

// Table represents a table in the schema
type Table struct {
	Name    string   `json:"name"`
	Columns []Column `json:"columns"`
}

// Schema represents the entire schema with multiple tables and the database name
type Schema struct {
	DatabaseName string  `json:"database_name"`
	Tables       []Table `json:"tables"`
}

// TrackingTable stores information on previously sent data
type TrackingTable struct {
	TableName  string
	LastSentID int64
}

// GetSchema uses GORM to fetch the schema
func GetSchema(db *gorm.DB, dbName string) (Schema, error) {
	var schema Schema
	schema.DatabaseName = dbName

	// Get the list of tables
	rows, err := db.Raw("SHOW TABLES").Rows()
	if err != nil {
		return schema, err
	}
	defer rows.Close()

	for rows.Next() {
		var tableName string
		err = rows.Scan(&tableName)
		if err != nil {
			return schema, err
		}

		tableName = fmt.Sprintf("`%s`", tableName)

		table := Table{Name: tableName}
		columnRows, err := db.Raw(fmt.Sprintf("DESCRIBE %s", tableName)).Rows()
		if err != nil {
			return schema, err
		}
		defer columnRows.Close()

		for columnRows.Next() {
			var field, colType, null, key string
			var defaultValue sql.NullString
			var extra string
			err = columnRows.Scan(&field, &colType, &null, &key, &defaultValue, &extra)
			if err != nil {
				return schema, err
			}

			column := Column{
				Name:       field,
				DataType:   colType,
				IsNullable: null == "YES",
				IsPrimary:  key == "PRI",
			}

			table.Columns = append(table.Columns, column)
		}

		schema.Tables = append(schema.Tables, table)
	}

	return schema, nil
}

// fetchData fetches data from a given table since the last sent ID
func fetchData(tableName string, lastSentID int64) ([]map[string]interface{}, int64, error) {
	var rows *sql.Rows
	var err error
	query := fmt.Sprintf("SELECT * FROM %s WHERE id > ? ORDER BY id ASC LIMIT 1000", tableName)
	rows, err = db.Raw(query, lastSentID).Rows()
	if err != nil {
		return nil, lastSentID, err
	}
	defer rows.Close()

	var data []map[string]interface{}
	var maxID int64

	for rows.Next() {
		columns, _ := rows.Columns()
		values := make([]interface{}, len(columns))
		for i := range values {
			values[i] = new(interface{})
		}

		err = rows.Scan(values...)
		if err != nil {
			return nil, lastSentID, err
		}

		entry := make(map[string]interface{})
		for i, col := range columns {
			entry[col] = *(values[i].(*interface{}))
		}
		data = append(data, entry)

		if id, ok := entry["id"].(int64); ok && id > maxID {
			maxID = id
		}
	}

	return data, maxID, nil
}

// publishDataToKafka publishes data to Kafka topic
func publishDataToKafka(writer *kafka.Writer, data []map[string]interface{}) error {
	for _, entry := range data {
		value, err := json.Marshal(entry)
		if err != nil {
			return err
		}

		err = writer.WriteMessages(context.Background(), kafka.Message{
			Value: value,
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// syncTable periodically syncs data from the Prism DB to Kafka
func syncTable(tableName string, tracking *TrackingTable, writer *kafka.Writer) {
	for {
		data, lastID, err := fetchData(tableName, tracking.LastSentID)
		if err != nil {
			log.Println("Error fetching data:", err)
			time.Sleep(10 * time.Second)
			continue
		}

		if len(data) > 0 {
			err = publishDataToKafka(writer, data)
			if err != nil {
				log.Println("Error sending data to Kafka:", err)
				time.Sleep(10 * time.Second)
				continue
			}

			tracking.LastSentID = lastID
			result := db.Exec("INSERT INTO tracking_table (table_name, last_sent_id) VALUES (?, ?) ON DUPLICATE KEY UPDATE last_sent_id = ?", tableName, tracking.LastSentID, tracking.LastSentID)
			if result.Error != nil {
				log.Println("Error executing query:", result.Error)
				return
			}

			// Optionally, check how many rows were affected
			rowsAffected := result.RowsAffected
			log.Printf("Rows affected: %d", rowsAffected)

			log.Printf("Data for table %s up to ID %d published successfully.\n", tableName, lastID)
		}

		time.Sleep(1 * time.Minute)
	}
}

func createTrackingTableIfNotExists() {
	result := db.Exec(`
        CREATE TABLE IF NOT EXISTS tracking_table (
            table_name VARCHAR(255) PRIMARY KEY,
            last_sent_id BIGINT NOT NULL
        );
    `)
	if result.Error != nil {
		log.Println("Error executing query:", result.Error)
		return
	}

	// Optionally, check how many rows were affected
	rowsAffected := result.RowsAffected
	log.Printf("Rows affected: %d", rowsAffected)
}

func main() {
	// MySQL connection string
	db, err = gorm.Open("mysql", "root:12345678@tcp(127.0.0.1:3306)/prism_db?charset=utf8&parseTime=True&loc=Local")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Specify the database name
	// databaseName := "prism_db"

	// // Get schema from MySQL using GORM
	// schema, err := GetSchema(db, databaseName)
	// if err != nil {
	// 	log.Fatal("Error fetching schema:", err)
	// }

	// // Serialize schema to JSON
	// schemaJSON, err := json.Marshal(schema)
	// if err != nil {
	// 	log.Fatal("Error serializing schema:", err)
	// }

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:          []string{"localhost:9092"},
		Topic:            "variant",
		Balancer:         &kafka.LeastBytes{},
		CompressionCodec: kafka.Lz4.Codec(),
		BatchSize:        500,
		BatchBytes:       2 * 1024 * 1024,
		MaxAttempts:      5,
		ReadTimeout:      time.Second * 5,
		WriteTimeout:     time.Second * 5,
		RequiredAcks:     int(kafka.RequireAll),
	})

	defer writer.Close()

	// // Send schema to Kafka
	// err = writer.WriteMessages(context.Background(), kafka.Message{
	// 	Value: schemaJSON,
	// })
	// if err != nil {
	// 	log.Fatal("Error sending message to Kafka:", err)
	// }

	// log.Println("Data published to Kafka successfully.")

	createTrackingTableIfNotExists()

	// Define the tables you want to track
	trackingTables := []TrackingTable{
		{TableName: "accounts", LastSentID: 0},
		{TableName: "account_balances", LastSentID: 0},
		{TableName: "attendance", LastSentID: 0},
		{TableName: "account_orders", LastSentID: 0},
		{TableName: "asset_masters", LastSentID: 0},
		// Add more tables as needed
	}

	// Start syncing each table concurrently
	for _, tracking := range trackingTables {
		go syncTable(tracking.TableName, &tracking, writer)
	}

	select {} // Block main goroutine
}
