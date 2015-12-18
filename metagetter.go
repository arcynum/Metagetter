package main

import (
	"fmt"
	"log"
	"time"
	"io/ioutil"
	"database/sql"
	_ "github.com/denisenkom/go-mssqldb"
	"os"
	"io"
	"encoding/csv"
	"encoding/json"
	"encoding/base64"
	"strconv"
	"path/filepath"
	"strings"
	"compress/gzip"
	"sync"
	"runtime"
)

// Define a waitgroup, to ensure all results are finished before continuing
var waitGroup sync.WaitGroup

// Database factory
func databaseConnectionFactory(connectionString string) (dbConnection* sql.DB) {
	dbConnection, err := sql.Open("mssql", connectionString)
	if err != nil {
		log.Fatal(err)
	}
	return dbConnection
}

// Main function.
func main() {

	// Start timing the script.
	start := time.Now()

	// Make go use more procs
	runtime.GOMAXPROCS(runtime.NumCPU())

	// Connection pools
	connectionPool := 10

	// Filepath separator
	sep := string(filepath.Separator)

	// Delta map
	deltaMap := make(map[string]time.Time)

	// Load the previous delta folder.
	deltaDate := findPreviousDelta("results")
	if deltaDate != "" {
		deltaCSV, err := os.Open("results" + sep + deltaDate + sep + "delta" + sep + "delta.csv")
		if err != nil {
			log.Println(err)
			return
		}
		defer deltaCSV.Close()

		reader := csv.NewReader(deltaCSV)
		for {
			row, err := reader.Read()
			if err != nil {
				if err == io.EOF {
					break
				}
				log.Println(err)
			}
			timestamp, err := time.Parse(time.RFC3339, row[2])
			deltaMap[row[0]] = timestamp
		}
	}

	// Load the config file.
	log.Println("Loading the configuration file")
	config, err := loadConfiguration("config.json");
	if err != nil {
		log.Println(err)
		return
	}

	// Create the server strings needed for the connection.
	var serverInst string
	if len(config.Instance) == 0 {
		serverInst = config.Server
	} else {
		serverInst = fmt.Sprintf("%s\\%s", config.Server, config.Instance)
	}

	// Create the password needed for the connection
	password, _ := base64.StdEncoding.DecodeString(config.Password)

	// Create the encryption string required.
	var encrypt string
	if len(config.Crypto) == 0 {
		encrypt = ""
	} else {
		encrypt = fmt.Sprintf(";encrypt=%s", config.Crypto)
	}

	// Create the connection string to the database.
	conString := fmt.Sprintf("server=%s;user id=%s;password=%s;database=%s;app name=metagetter;%s",
		serverInst,
		config.Username,
		password,
		config.Database,
		encrypt,
	)

	// Create the connection to the database.
	log.Println("Opening a database connection")
	dbConnection := databaseConnectionFactory(conString)
	defer dbConnection.Close()

	// The parent container which holds all the metadata.
	tableContainer := make([]Table, 0)

	// Create the table slice.
	var tables []string

	// Need to check the mode and include/exclude tables.
	switch config.Mode {
		case "whitelist": {
			log.Println("Using the table whitelist")
			tables = config.Whitelist
		}
		case "blacklist": {
			log.Println("Fetching the SQL table list")
			tables = getTables(config.Database, config.Blacklist, dbConnection)
		}
	}

	// Check if the results folder exists
	err = os.Mkdir("results", 0777)
	if err != nil {
		log.Println(fmt.Sprintf("Unable to create the directory: %s", "results"))
	}

	// Create the folder timestamp
	t := time.Now().Local()
	timeFol := t.Format("2006_01_02")
	err = os.Mkdir("results" + sep +  timeFol, 0777)
	if err != nil {
		log.Println(fmt.Sprintf("Unable to create the directory: %s", "results" + sep +  timeFol))
	}

	// Create the base folder object
	base := "results" + sep +  timeFol + sep

	// Create the metadata folder
	err = os.Mkdir(base + "metadata", 0777)
	if err != nil {
		log.Println(fmt.Sprintf("Unable to create the directory: %s", base + "metadata"))
	}

	// Create the tables folder
	err = os.Mkdir(base + "tables", 0777)
	if err != nil {
		log.Println(fmt.Sprintf("Unable to create the directory: %s", base + "tables"))
	}

	// Create the describe folder
	err = os.Mkdir(base + "describe", 0777)
	if err != nil {
		log.Println(fmt.Sprintf("Unable to create the directory: %s", base + "describe"))
	}

	// Create the tables folder
	err = os.Mkdir(base + "delta", 0777)
	if err != nil {
		log.Println(fmt.Sprintf("Unable to create the directory: %s", base + "delta"))
	}

	// Loop through the table and run the queries
	log.Println("Getting the metadata and row counts")
	for _, table := range tables {
		result := getTableMetadata(table, dbConnection)
		result.rowCount = getRowCount(table, dbConnection)
		result.folder = base + "tables"
		tableContainer = append(tableContainer, result)
	}

	// Loop through the results and write out CSV files.
	log.Println("Writing out the metadata to disk")
	for _, table := range tableContainer {

		// Create the CSV file handle.
		outFile, err := os.Create(base + "metadata" + sep + table.name + ".csv")
		if err != nil {
			fmt.Println(err)
			return
		}
		defer outFile.Close()

		// Create the CSV writer from the file handle.
		writer := csv.NewWriter(outFile)

		// Write the header row.
		writer.Write(
			[]string {
				"Column Name",
				"Data Type",
				"Max Length",
				"Precision",
				"Scale",
				"Nullable",
				"Ordinal Position",
				"Collation Name",
				"Primary Key",
				"Row Count",
			},
		)

		// Write each column and its data out to the file.
		for _, column := range table.columns {
			writer.Write([]string{
				column.name.String,
				column.dataType.String,
				column.maxLength.String,
				column.precision.String,
				column.scale.String,
				column.nullable.String,
				column.ordinalPosition.String,
				column.collationName.String,
				column.primaryKey.String,
				strconv.Itoa(table.rowCount),
			})

			// Flush the current row out to the file.
			writer.Flush()
		}
	}

	// Loop through the results and write out the describe statements
	log.Println("Writing out the describes to disk")
	for _, table := range tableContainer {

		// Create the CSV file handle.
		outFile, err := os.Create(base + "describe" + sep + table.name + ".sql")
		if err != nil {
			fmt.Println(err)
			return
		}
		defer outFile.Close()

		// Write the header row.
		outFile.WriteString(fmt.Sprintf("CREATE TABLE [%s] (\n", table.name))

		// Write each column and its data out to the file.
		for _, column := range table.columns {

			var dataType string
			switch column.dataType.String {
				case "bigint":
					dataType = fmt.Sprintf("[%s]", column.dataType.String)
				case "binary":
					dataType = fmt.Sprintf("[%s](%s)", column.dataType.String, column.maxLength.String)
				case "bit":
					dataType = fmt.Sprintf("[%s]", column.dataType.String)
				case "char":
					dataType = fmt.Sprintf("[%s](%s)", column.dataType.String, column.maxLength.String)
				case "date":
					dataType = fmt.Sprintf("[%s]", column.dataType.String)
				case "datetime":
					dataType = fmt.Sprintf("[%s]", column.dataType.String)
				case "datetimeoffset":
					dataType = fmt.Sprintf("[%s](%s)", column.dataType.String, column.maxLength.String)
				case "decimal":
					dataType = fmt.Sprintf("[%s](%s, %s)", column.dataType.String, column.precision.String, column.scale.String)
				case "float":
					dataType = fmt.Sprintf("[%s]", column.dataType.String)
				case "geography":
					dataType = fmt.Sprintf("[%s]", column.dataType.String)
				case "geometry":
					dataType = fmt.Sprintf("[%s]", column.dataType.String)
				case "hierarchyid":
					dataType = fmt.Sprintf("[%s]", column.dataType.String)
				case "image":
					dataType = fmt.Sprintf("[%s]", column.dataType.String)
				case "int":
					dataType = fmt.Sprintf("[%s]", column.dataType.String)
				case "money":
					dataType = fmt.Sprintf("[%s]", column.dataType.String)
				case "nchar":
					dataType = fmt.Sprintf("[%s](%s)", column.dataType.String, column.maxLength.String)
				case "ntext":
					dataType = fmt.Sprintf("[%s]", column.dataType.String)
				case "numeric":
					dataType = fmt.Sprintf("[%s](%s, %s)", column.dataType.String, column.precision.String, column.scale.String)
				case "nvarchar":
					dataType = fmt.Sprintf("[%s](%s)", column.dataType.String, column.maxLength.String)
				case "real":
					dataType = fmt.Sprintf("[%s]", column.dataType.String)
				case "smalldatetime":
					dataType = fmt.Sprintf("[%s]", column.dataType.String)
				case "smallint":
					dataType = fmt.Sprintf("[%s]", column.dataType.String)
				case "smallmoney":
					dataType = fmt.Sprintf("[%s]", column.dataType.String)
				case "sql_variant":
					dataType = fmt.Sprintf("[%s]", column.dataType.String)
				case "text":
					dataType = fmt.Sprintf("[%s]", column.dataType.String)
				case "time":
					dataType = fmt.Sprintf("[%s](%s)", column.dataType.String, column.maxLength.String)
				case "timestamp":
					dataType = fmt.Sprintf("[%s]", column.dataType.String)
				case "tinyint":
					dataType = fmt.Sprintf("[%s]", column.dataType.String)
				case "uniqueidentifier":
					dataType = fmt.Sprintf("[%s]", column.dataType.String)
				case "varbinary":
					dataType = fmt.Sprintf("[%s](%s)", column.dataType.String, column.maxLength.String)
				case "varchar":
					dataType = fmt.Sprintf("[%s](%s)", column.dataType.String, column.maxLength.String)
				case "xml":
					dataType = fmt.Sprintf("[%s]", column.dataType.String)
				default:
					dataType = fmt.Sprintf("[%s]", column.dataType.String)
			}

			var null string
			switch column.nullable.String {
				case "true":
					null = "NULL"
				case "false":
					null = "NOT NULL"
				default:
					null = "NULL"
			}

			var primaryKey string
			switch column.primaryKey.String {
				case "true":
					primaryKey = " PRIMARY KEY"
				case "false":
					primaryKey = ""
				default:
					primaryKey = ""
			}

			outFile.WriteString(fmt.Sprintf("\t[%s] %s %s%s,\n",
				column.name.String,
				dataType,
				null,
				primaryKey,
			))
		}

		outFile.WriteString(");")
	}

	// Determine if the table is a TYPE 2 or not.
	log.Println("Type 2 Tables")
	for index, table := range tableContainer {
		for _, type2 := range config.Type2 {
			if strings.ToUpper(table.name) == type2 {
				tableContainer[index].type2 = true
				log.Println(table.name)
			}
		}
	}

	// Determine the delta which is used in the name, from the config heirarchy.
	// Don't bother if its a known type 2 table.
	for index, table := range tableContainer {
		if !table.type2 {
			for _, column := range table.columns {
				for _, timestamp := range config.Timestamps {
					if strings.ToUpper(column.name.String) == timestamp {
						tableContainer[index].timestamp = strings.ToUpper(column.name.String)
					}
				}
			}
		}
	}

	// Loop through the tables and generate the actual data.
	log.Println("Writing out the deltas")
	writeDeltas(tableContainer, base + "delta", dbConnection)

	var inputChannel = make(chan Table)

	// Spawn the worker goroutines for the processing
	for i := 1; i <= connectionPool; i++ {
		waitGroup.Add(1)
		go getTableData(inputChannel, conString, i)
	}

	// Loop through the tables and generate the actual data.
	log.Println("Starting the data download")
	for _, table := range tableContainer {
		if table.rowCount > 0 {
			deltaTime := deltaMap[table.name]
			if deltaTime.IsZero() || table.type2 {
				table.where = ""
			} else {
				table.where = deltaTime.Format(time.RFC3339)
			}
			inputChannel <- table
		}
	}
	close(inputChannel)

	// Wait for all addresses to resolve
	waitGroup.Wait()

	// Print the final time the program ran.
	fmt.Printf("Program ran in %s", time.Since(start))
}

func loadConfiguration(path string) (*Config, error) {
	// Load the config file from disk.
	configFile, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	// Unmarshal the config file.
	var config Config
	err = json.Unmarshal(configFile, &config)
	if err != nil {
		return nil, err
	}
	return &config, nil
}

func writeDeltas(tables []Table, folder string, dbConnection* sql.DB) {

	// Create the CSV file handle.
	outFile, err := os.Create(folder + string(filepath.Separator) + "delta.csv")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer outFile.Close()

	// Create the CSV writer from the file handle.
	writer := csv.NewWriter(outFile)

	// Write the header row.
	writer.Write(
		[]string{
			"TABLE_NAME",
			"COLUMN_NAME",
			"MAX_TIMESTAMP",
			"TOTAL_RECORDS",
		},
	)
	writer.Flush()

	for _, table := range tables {

		if table.rowCount == 0 {
			continue
		}

		if table.timestamp != "" {
			timestamp := getMaxTimestamp(table.name, table.timestamp, dbConnection)

			if timestamp.IsZero() {
				continue
			}

			// Write each column and its data out to the file.
			writer.Write([]string{
				table.name,
				table.timestamp,
				timestamp.Format(time.RFC3339),
				strconv.Itoa(table.rowCount),
			})

			// Flush the current row out to the file.
			writer.Flush()
		}
	}
}

func getTableData(tables <-chan Table, conString string, worker int) {

	// Get a table out of the channel to process
	for table := range tables {

		// Sleep for a moment to give up threadlock
		time.Sleep(100 * time.Millisecond)

		// Log the current state.
		log.Println(fmt.Sprintf("Processing table %s on thread %v", table.name, worker))

		// DB Connection Object
		dbConnection := databaseConnectionFactory(conString);

		// Build select order
		var columnList string
		for index, column := range table.columns {
			var columnName string

			// Dealing with the service manager "image" types, which are actually binary data we can't read yet.
			if column.dataType.String == "image" {
				columnName = "'{img}' as [" + column.name.String + "]"
			} else {
				columnName = "[" + column.name.String + "]"
			}

			if index == 0 {
				columnList += columnName
			} else {
				columnList += "," + columnName
			}
		}

		where := ""
		if table.where != "" {
			where = fmt.Sprintf("WHERE %s >= '%s'", table.timestamp, table.where)
			log.Println(where)
		}

		// Final query string for getting the database values.
		queryString := fmt.Sprintf("SELECT %s FROM [%s] %s", columnList, table.name, where)

		// Open the database query and get ready to read results.
		query, err := dbConnection.Query(queryString)
		if err != nil {
			log.Fatal(err)
			break
		}
		defer query.Close()

		// Create the CSV file handle.
		outFile, err := os.Create(table.folder + string(filepath.Separator) + table.name + ".csv.gz")
		if err != nil {
			log.Fatal(err)
			break
		}
		defer outFile.Close()

		// Create the GZIP file handle.
		gzwriter, err := gzip.NewWriterLevel(outFile, gzip.DefaultCompression)
		if err != nil {
			log.Fatal(err)
			break
		}

		// Create the CSV writer from the file handle.
		writer := csv.NewWriter(gzwriter)

		// Go through the results and create an array of results.
		for query.Next() {

			// Create interface result
			var dataInterface []interface{}

			// Loop through and create the return interfaces
			for i := 0; i < len(table.columns); i++ {

				// Interface for the interface.
				dataInterface = append(dataInterface, new(interface{}))
			}

			// Create the container of row data
			data := make([]string, 0)
			err := query.Scan(dataInterface...)
			if err != nil {
				log.Fatal(err)
				break
			}

			// Loop through the interface and double dereference the interfaces to check the type.
			for i := 0; i < len(table.columns); i++ {

				var r string
				switch v := (*dataInterface[i].(*interface{})).(type) {
					case time.Time:
						r = v.Format(time.RFC3339)
					case nil:
						r = ""
					case float64:
						r = fmt.Sprintf("%.2f", v)
					case int:
						r = fmt.Sprintf("%v", v)
					case int8:
						r = fmt.Sprintf("%v", v)
					case int16:
						r = fmt.Sprintf("%v", v)
					case int32:
						r = fmt.Sprintf("%v", v)
					case int64:
						r = fmt.Sprintf("%v", v)
					case []byte:
						r = string(v)
					default:
						if str, ok := v.(string); ok {
							r = str
						} else {
							r = "<unknown type>"
						}
				}

				data = append(data, r)
			}

			writer.Write(data)
			writer.Flush()
		}

		// Force close this connection to get another one from the pool.
		dbConnection.Close()
	}

	// Remove an entry from the waitgroup.
	log.Println(fmt.Sprintf("Thread %v ending", worker))
	waitGroup.Done()
}

// If a folder does not exist, create it.
func createFolder(path string) (bool, error) {
	folderExists, err := exists(path)
	if err != nil {
		return false, err
	}
	if !folderExists {
		err := os.Mkdir(path, 0777)
		if err != nil {
			return false, err
		}
	}
	return true, nil
}

// Returns whether the given file or directory exists or not.
func exists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// Find the last delta csv
func findPreviousDelta(path string) string {
	folders, _ := ioutil.ReadDir(path)

	if len(folders) == 0 {
		return ""
	}

	dates := make([]time.Time, 0)
	for _, item := range folders {
		date, _ := time.Parse("2006_01_02", item.Name())
		dates = append(dates, date)
	}

	log.Println(dates);

	// Get today
	now := time.Now()

	// The final date
	var folder time.Time
	breakout := false

	// Loop through the dates and find the closest to
	i := -1
	for {
		// log.Println(fmt.Sprintf("Checking %s", now.AddDate(0, 0, i).Format("2006-01-02")))
		for _, date := range dates {
			if breakout {
				break
			}
			if now.AddDate(0, 0, i).Year() == date.Year() && 
			now.AddDate(0, 0, i).Month() == date.Month() &&
			now.AddDate(0, 0, i).Day() == date.Day() {
				folder = date
				breakout = true
			}
		}
		if breakout {
			break
		}
		i = i - 1
	}

	log.Println("The oldest folder is:")
	log.Println(folder.Format("2006_01_02"))

	return folder.Format("2006_01_02")
}