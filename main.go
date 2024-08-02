package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"
	"os"
	"path/filepath"

	_ "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
)


type Employee struct {
	EmpID     string    `json:"empId"`
	Name      string    `json:"name"`
	Age       int       `json:"age"`
	Salary    float64   `json:"salary"`
	City      string    `json:"city"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}
type FileInfo struct {
	Name string
	Size int64
}

var db *sql.DB

func main() {
	var err error
	db, err = sql.Open("mysql", "root:00000000@tcp(localhost:3306)/emp?parseTime=true")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	//added for CORS -Next Js
	mux := http.NewServeMux()
	mux.HandleFunc("/employee", getEmployees)
	mux.HandleFunc("/employee-by-salary", getEmployeesBySalary)
	mux.HandleFunc("/employee-by-age", getEmployeesByAge)
	mux.HandleFunc("/employee-top/", getTopEmployees)
	mux.HandleFunc("/most-payed-city", getMostPayedCity)
	mux.HandleFunc("/avg-salary/", getAvgSalaryByCity)
	mux.HandleFunc("/employee-count-per-city", getEmployeeCountPerCity)
	mux.HandleFunc("/employee-age-between/", getEmployeesByAgeRange)
	mux.HandleFunc("/city-salary-percentage", getCitySalaryPercentage)
	mux.HandleFunc("/employee-create", createEmployee)
	mux.HandleFunc("/employee-update/", updateEmployee)
	mux.HandleFunc("/upload", uploadHandler)
	// http.HandleFunc("/upload", uploadFile)
	handler := enableCORS(mux)
	log.Fatal(http.ListenAndServe(":8080", handler))
}

func getEmployees(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	rows, err := db.Query("SELECT * FROM employees ORDER BY created_at")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var employees []Employee
	for rows.Next() {
		var e Employee
		err := rows.Scan(&e.EmpID, &e.Name, &e.Age, &e.Salary, &e.City, &e.CreatedAt, &e.UpdatedAt)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		employees = append(employees, e)
	}

	json.NewEncoder(w).Encode(employees)
}

func uploadHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse the multipart form, 10 << 20 specifies a maximum upload of 10 MB
	err := r.ParseMultipartForm(10 << 20)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	file, handler, err := r.FormFile("file")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer file.Close()

	// Create the uploads folder if it doesn't exist
	err = os.MkdirAll("./uploads", os.ModePerm)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Create a new file in the uploads directory
	dst, err := os.Create(filepath.Join("uploads", handler.Filename))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer dst.Close()

	// Copy the uploaded file to the created file on the filesystem
	if _, err := io.Copy(dst, file); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Get the file size
	fileInfo, err := dst.Stat()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Store file information
	uploadedFile := FileInfo{
		Name: handler.Filename,
		Size: fileInfo.Size(),
	}

	
	log.Printf("Uploaded File: name=%s, size=%d bytes", uploadedFile.Name, uploadedFile.Size)

	fmt.Fprintf(w, "Successfully uploaded file\nName: %s\nSize: %d bytes", uploadedFile.Name, uploadedFile.Size)
}

func getEmployeesBySalary(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	rows, err := db.Query("SELECT * FROM employees ORDER BY salary DESC")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var employees []Employee
	for rows.Next() {
		var e Employee
		err := rows.Scan(&e.EmpID, &e.Name, &e.Age, &e.Salary, &e.City, &e.CreatedAt, &e.UpdatedAt)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		employees = append(employees, e)
	}

	json.NewEncoder(w).Encode(employees)
}

func getEmployeesByAge(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	rows, err := db.Query("SELECT * FROM employees ORDER BY age, name")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var employees []Employee
	for rows.Next() {
		var e Employee
		err := rows.Scan(&e.EmpID, &e.Name, &e.Age, &e.Salary, &e.City, &e.CreatedAt, &e.UpdatedAt)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		employees = append(employees, e)
	}

	json.NewEncoder(w).Encode(employees)
}

func getTopEmployees(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	parts := strings.Split(r.URL.Path, "/")
	if len(parts) != 3 {
		http.Error(w, "Invalid URL", http.StatusBadRequest)
		return
	}

	topNumber, err := strconv.Atoi(parts[2])
	if err != nil {
		http.Error(w, "Invalid number", http.StatusBadRequest)
		return
	}

	rows, err := db.Query("SELECT empId, name FROM employees ORDER BY salary DESC LIMIT ?", topNumber)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var results []struct {
		EmpID string `json:"empId"`
		Name  string `json:"name"`
	}

	for rows.Next() {
		var r struct {
			EmpID string `json:"empId"`
			Name  string `json:"name"`
		}
		err := rows.Scan(&r.EmpID, &r.Name)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		results = append(results, r)
	}

	json.NewEncoder(w).Encode(results)
}

func getMostPayedCity(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var city string
	var totalSalary float64
	err := db.QueryRow("SELECT city, SUM(salary) as total_salary FROM employees GROUP BY city ORDER BY total_salary DESC LIMIT 1").Scan(&city, &totalSalary)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	result := struct {
		City        string  `json:"city"`
		TotalSalary float64 `json:"totalSalary"`
	}{
		City:        city,
		TotalSalary: totalSalary,
	}

	json.NewEncoder(w).Encode(result)
}

func getAvgSalaryByCity(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	parts := strings.Split(r.URL.Path, "/")
	if len(parts) != 3 {
		http.Error(w, "Invalid URL", http.StatusBadRequest)
		return
	}

	city := parts[2]

	var avgSalary float64
	err := db.QueryRow("SELECT AVG(salary) FROM employees WHERE city = ?", city).Scan(&avgSalary)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	result := struct {
		City       string  `json:"city"`
		AvgSalary  float64 `json:"avgSalary"`
	}{
		City:       city,
		AvgSalary:  avgSalary,
	}

	json.NewEncoder(w).Encode(result)
}

func getEmployeeCountPerCity(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	rows, err := db.Query("SELECT city, COUNT(*) as count FROM employees GROUP BY city")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var results []struct {
		City  string `json:"city"`
		Count int    `json:"count"`
	}

	for rows.Next() {
		var r struct {
			City  string `json:"city"`
			Count int    `json:"count"`
		}
		err := rows.Scan(&r.City, &r.Count)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		results = append(results, r)
	}

	json.NewEncoder(w).Encode(results)
}

func getEmployeesByAgeRange(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	parts := strings.Split(r.URL.Path, "/")
	if len(parts) != 4 {
		http.Error(w, "Invalid URL", http.StatusBadRequest)
		return
	}

	fromAge, err := strconv.Atoi(parts[2])
	if err != nil {
		http.Error(w, "Invalid from_age", http.StatusBadRequest)
		return
	}

	toAge, err := strconv.Atoi(parts[3])
	if err != nil {
		http.Error(w, "Invalid to_age", http.StatusBadRequest)
		return
	}

	rows, err := db.Query("SELECT * FROM employees WHERE age BETWEEN ? AND ?", fromAge, toAge)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var employees []Employee
	for rows.Next() {
		var e Employee
		err := rows.Scan(&e.EmpID, &e.Name, &e.Age, &e.Salary, &e.City, &e.CreatedAt, &e.UpdatedAt)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		employees = append(employees, e)
	}

	json.NewEncoder(w).Encode(employees)
}

func getCitySalaryPercentage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	rows, err := db.Query("SELECT city, SUM(salary) as total_salary, (SUM(salary) / (SELECT SUM(salary) FROM employees)) * 100 as percentage FROM employees GROUP BY city")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var results []struct {
		City       string  `json:"city"`
		TotalSalary float64 `json:"totalSalary"`
		Percentage  float64 `json:"percentage"`
	}

	for rows.Next() {
		var r struct {
			City       string  `json:"city"`
			TotalSalary float64 `json:"totalSalary"`
			Percentage  float64 `json:"percentage"`
		}
		err := rows.Scan(&r.City, &r.TotalSalary, &r.Percentage)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		results = append(results, r)
	}

	json.NewEncoder(w).Encode(results)
}

func createEmployee(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var e Employee
	err := json.NewDecoder(r.Body).Decode(&e)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	e.EmpID = uuid.New().String()
	e.CreatedAt = time.Now()
	e.UpdatedAt = e.CreatedAt

	_, err = db.Exec("INSERT INTO employees (empId, name, age, salary, city, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
		e.EmpID, e.Name, e.Age, e.Salary, e.City, e.CreatedAt, e.UpdatedAt)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(e)
}

func updateEmployee(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	parts := strings.Split(r.URL.Path, "/")
	if len(parts) != 3 {
		http.Error(w, "Invalid URL", http.StatusBadRequest)
		return
	}

	empID := parts[2]

	var e Employee
	err := json.NewDecoder(r.Body).Decode(&e)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return 
	}

	_, err = db.Exec("UPDATE employees SET salary = ?, city = ?, updated_at = ? WHERE empId = ?",
		e.Salary, e.City, time.Now(), empID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(e)
}

func enableCORS(next http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        w.Header().Set("Access-Control-Allow-Origin", "*") // Allow all origins; modify this for more security
        w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
        w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

        // Handle preflight requests
        if r.Method == http.MethodOptions {
            return
        }

        next.ServeHTTP(w, r)
    })
}