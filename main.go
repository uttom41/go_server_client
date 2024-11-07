package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"database/sql"

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

// GetSchema uses GORM to fetch the schema
func GetSchema(db *gorm.DB, dbName string) (Schema, error) {
	var schema Schema
	schema.DatabaseName = dbName

	// Get the list of tables using GORM
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

		// Escape the table name to handle reserved keywords
		tableName = fmt.Sprintf("`%s`", tableName)

		// Get columns for each table
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

func main() {
	// MySQL connection string
	db, err = gorm.Open("mysql", "root:12345678@tcp(127.0.0.1:3306)/prism?charset=utf8&parseTime=True&loc=Local")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Specify the database name
	databaseName := "prism"

	// Get schema from MySQL using GORM
	schema, err := GetSchema(db, databaseName)
	if err != nil {
		log.Fatal("Error fetching schema:", err)
	}

	// Serialize schema to JSON
	schemaJSON, err := json.Marshal(schema)
	if err != nil {
		log.Fatal("Error serializing schema:", err)
	}

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:          []string{"localhost:9092"},
		Topic:            "variant",
		Balancer:         &kafka.LeastBytes{},
		CompressionCodec: kafka.Lz4.Codec(),
		BatchSize:        500,          // Reduce if necessary to control message size
		BatchBytes:       2 * 1024 * 1024, // 1MB (or set appropriately)
		RequiredAcks:     int(kafka.RequireAll),
	})

	defer writer.Close()

	// Send schema to Kafka
	err = writer.WriteMessages(context.Background(), kafka.Message{
		Value: schemaJSON,
	})
	if err != nil {
		log.Fatal("Error sending message to Kafka:", err)
	}

	log.Println("Data published to Kafka successfully.")
}
