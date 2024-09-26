package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"strings"
	"time"
	"unicode"

	"github.com/PuerkitoBio/goquery"
	"github.com/gocolly/colly"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	_ "github.com/lib/pq"
	"golang.org/x/time/rate"
)

// Define your struct
type item struct {
	ID        uuid.UUID `json:"id"`
	Category  string    `json:"category"`
	Name      string    `json:"name"`
	Address   string    `json:"address"`
	Type      string    `json:"type"`
	Domain    string    `json:"domain"`
	Timestamp string    `json:"timestamp"`
	Date      string    `json:"date"`
}

// Database connection (PostgreSQL)
var db *sql.DB

var visitors = make(map[string]*rate.Limiter)
var mu sync.Mutex // Guards visitors map

// Rate limiter settings (5 requests per minute)
const requestsPerMinute = 5
const burstLimit = 5

func main() {
	// Initialize DB
	initDB()

	// Start the background scraping job
	go startScrapingJob()

	// Set up your HTTP router
	router := mux.NewRouter()

	router.Use(rateLimitMiddleware)

	router.HandleFunc("/reports", getReports).Methods("GET")
	router.HandleFunc("/reports/{id}", getReportByID).Methods("GET")

	fmt.Println("Server started at :8080")
	http.ListenAndServe(":8080", router)
}

func rateLimitMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ip := getIP(r)

		limiter := getVisitorLimiter(ip)
		if !limiter.Allow() {
			http.Error(w, "Too many requests. Please try again later.", http.StatusTooManyRequests)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// Get the rate limiter for a specific IP address
func getVisitorLimiter(ip string) *rate.Limiter {
	mu.Lock()
	defer mu.Unlock()

	limiter, exists := visitors[ip]
	if !exists {
		limiter = rate.NewLimiter(rate.Every(time.Minute/requestsPerMinute), burstLimit)
		visitors[ip] = limiter
	}

	return limiter
}

// Get the IP address of the client
func getIP(r *http.Request) string {
	ip := r.Header.Get("X-Forwarded-For") // Get IP from reverse proxy or load balancer
	if ip == "" {
		ip = r.RemoteAddr // Fallback to direct IP
	}
	// Strip port number from RemoteAddr (IP:port format)
	if idx := strings.LastIndex(ip, ":"); idx != -1 {
		ip = ip[:idx]
	}
	return ip
}

func initDB() {
	// Connect to the PostgreSQL database
	connStr := "postgresql://fraudreports_user:L00CF6BLnnjdWvgzCmyLvPk6KxCgD4q7@dpg-crqi74o8fa8c7392ic10-a.oregon-postgres.render.com/fraudreports"
	var err error
	db, err = sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}
	// Ensure the table exists
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS reports (
		id UUID PRIMARY KEY,
		category VARCHAR(255),
		name VARCHAR(255),
		address VARCHAR(255),
		type VARCHAR(50),
		domain VARCHAR(255),
		timestamp VARCHAR(50),
		date VARCHAR(50)
	)`)
	if err != nil {
		log.Fatal(err)
	}
}

// Start the background job to run scraping every 5 minutes
func startScrapingJob() {
	for {
		// Run scraping job
		fmt.Println("Starting scraping job...")
		createReports()

		// Sleep for 5 minutes before the next job
		time.Sleep(15 * time.Minute)
	}
}

// Web scraping function that extracts reports and stores them in the DB
func createReports() {
	var reports []item
	c := colly.NewCollector()

	c.OnHTML(".create-ScamReportCard", func(e *colly.HTMLElement) {
		Category := e.ChildText(".create-ScamReportCard__category-section p")
		Name := e.ChildText(".create-ScamReportCard__preview-description-wrapper")
		Address := e.ChildText(".create-ReportedSection__address-section .create-ResponsiveAddress__text")
		Domain := e.ChildText(".create-ReportedSection__domain")
		Timestamp := e.ChildText(".create-ScamReportCard__submitted-info > span:nth-child(3)")

		imgAlt := ""
		e.DOM.Find(".create-ReportedSection__address-section img").Each(func(_ int, img *goquery.Selection) {
			altText, exists := img.Attr("alt")
			if exists {
				imgAlt = altText
			}
		})

		if imgAlt != "" {
			words := strings.Fields(imgAlt) // Split the string into words
			if len(words) > 0 {
				imgAlt = words[0] // Get the first word
			}
		}

		name := processNameField(Name)
		timestamp := parseTime(Timestamp)
		t, err := time.Parse(time.RFC3339, timestamp)
		if err != nil {
			fmt.Println("Error parsing time:", err)
			return
		}

		date := t.Format("2006-01-02")
		Timestamp = t.Format("15:04:05")

		report := item{
			ID:        uuid.New(),
			Category:  Category,
			Name:      name,
			Address:   Address,
			Type:      imgAlt,
			Domain:    Domain,
			Timestamp: Timestamp,
			Date:      date,
		}

		reports = append(reports, report)

		// Insert into DB
		_, err = db.Exec("INSERT INTO reports (id, category, name, address, type, domain, timestamp, date) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
			report.ID, report.Category, report.Name, report.Address, report.Type, report.Domain, report.Timestamp, report.Date)
		if err != nil {
			log.Fatal(err)
		}
	})

	// Visit the website for scraping
	c.Visit("https://www.chainabuse.com/reports")

	fmt.Println("Scraping job completed.")
}

// Fetch all reports from the DB
func getReports(w http.ResponseWriter, r *http.Request) {
	rows, err := db.Query("SELECT id, category, name, address, type, domain, timestamp, date FROM reports")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	var reports []item
	for rows.Next() {
		var report item
		err := rows.Scan(&report.ID, &report.Category, &report.Name, &report.Address, &report.Type, &report.Domain, &report.Timestamp, &report.Date)
		if err != nil {
			log.Fatal(err)
		}
		reports = append(reports, report)
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(reports)
}

// Fetch a single report by ID
func getReportByID(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	id := params["id"]

	var report item
	err := db.QueryRow("SELECT id, category, name, address, type, domain, timestamp, date FROM reports WHERE id = $1", id).
		Scan(&report.ID, &report.Category, &report.Name, &report.Address, &report.Type, &report.Domain, &report.Timestamp, &report.Date)
	if err != nil {
		http.Error(w, "Report not found", http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(report)
}

// Utility functions to process time and name field
func parseTime(relativeTime string) string {
	currentDate := time.Now()
	fields := strings.Fields(relativeTime)
	if len(fields) < 2 {
		return ""
	}

	amount := fields[0]
	unit := fields[1]

	if strings.Contains(unit, "minute") {
		duration, _ := time.ParseDuration(fmt.Sprintf("-%sm", amount))
		currentDate = currentDate.Add(duration)
	} else if strings.Contains(unit, "hour") {
		duration, _ := time.ParseDuration(fmt.Sprintf("-%sh", amount))
		currentDate = currentDate.Add(duration)
	} else if strings.Contains(unit, "second") {
		duration, _ := time.ParseDuration(fmt.Sprintf("-%ss", amount))
		currentDate = currentDate.Add(duration)
	}

	return currentDate.Format(time.RFC3339)
}

func processNameField(name string) string {
	name = strings.TrimSpace(name)
	if strings.Contains(name, "@") {
		parts := strings.Split(name, "@")
		if len(parts) > 1 {
			afterAt := strings.Fields(parts[1])
			if len(afterAt) > 0 {
				return afterAt[0]
			}
		}
	}
	words := strings.FieldsFunc(name, func(c rune) bool {
		return !unicode.IsLetter(c) && !unicode.IsDigit(c)
	})
	if len(words) == 1 {
		return words[0]
	}
	return ""
}
