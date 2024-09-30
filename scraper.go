package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/PuerkitoBio/goquery"
	"github.com/chromedp/chromedp"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	_ "github.com/lib/pq"
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

var db *sql.DB

func main() {
	// Initialize DB
	initDB()

	// Start the background scraping job
	go startScrapingJob()

	// Set up your HTTP router
	router := mux.NewRouter()

	router.HandleFunc("/reports", getReports).Methods("GET")
	router.HandleFunc("/reports/{id}", getReportByID).Methods("GET")

	fmt.Println("Server started at :8080")
	http.ListenAndServe(":8080", router)
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
		fmt.Println("Starting scraping job...")
		createReports()

		// Sleep for 5 minutes before the next job
		time.Sleep(60 * time.Minute)
	}
}

func createReports() {
	totalReports, err := getTotalReports()
	if err != nil {
		log.Fatal(err)
		return
	}

	totalPages := (totalReports / 15) + 1
	fmt.Printf("Total reports: %d, Total pages: %d\n", totalReports, totalPages)

	// Iterate over each page
	for i := 0; i < totalPages; i++ {
		pageURL := fmt.Sprintf("https://www.chainabuse.com/reports?page=%d", i)
		fmt.Printf("Scraping page: %d\n", i+1)
		scrapePage(pageURL)
	}
}

func scrapePage(url string) {
	ctx, cancel := chromedp.NewContext(context.Background())
	defer cancel()

	var reports []item
	var htmlContent string

	// Navigate to the page and get the outer HTML
	err := chromedp.Run(ctx,
		chromedp.Navigate(url),
		chromedp.WaitVisible(".create-ScamReportCard"),
		chromedp.OuterHTML("html", &htmlContent),
	)

	if err != nil {
		log.Fatalf("Error navigating to %s: %v", url, err)
	}

	// Parse the HTML with goquery to extract the reports
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(htmlContent))
	if err != nil {
		log.Fatalf("Error loading HTML document: %v", err)
	}

	doc.Find(".create-ScamReportCard").Each(func(i int, e *goquery.Selection) {
		Category := e.Find(".create-ScamReportCard__category-section p").Text()
		Name := e.Find(".create-ScamReportCard__preview-description-wrapper").Text()
		Address := e.Find(".create-ReportedSection__address-section .create-ResponsiveAddress__text").Text()
		Domain := e.Find(".create-ReportedSection__domain").Text()
		Timestamp := e.Find(".create-ScamReportCard__submitted-info > span:nth-child(3)").Text()

		// Handle type from img alt text
		imgAlt := ""
		e.Find(".create-ReportedSection__address-section img").Each(func(_ int, img *goquery.Selection) {
			altText, exists := img.Attr("alt")
			if exists {
				imgAlt = altText
			}
		})

		if imgAlt != "" {
			words := strings.Fields(imgAlt)
			if len(words) > 0 {
				imgAlt = words[0]
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

		// Check if the report already exists in the database
		var exists bool
		err = db.QueryRow(`
			SELECT EXISTS(
				SELECT 1 FROM reports
				WHERE category = $1
				AND name = $2
				AND address = $3
				AND type = $4
				AND domain = $5
			)`, Category, name, Address, imgAlt, Domain).Scan(&exists)

		if err != nil {
			fmt.Println("Error querying database:", err)
			return
		}

		if exists {
			fmt.Printf("Report with category %s and address %s already exists. Skipping insertion.\n", Category, Address)
			return
		}

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

		maxLength := 1024
		if len(report.Category) > maxLength {
    		report.Category = report.Category[:maxLength]
		}
		if len(report.Name) > maxLength {
    		report.Name = report.Name[:maxLength]
		}
		if len(report.Address) > maxLength {
    		report.Address = report.Address[:maxLength]
		}
		if len(report.Type) > maxLength {
    		report.Type = report.Type[:maxLength]
		}
		if len(report.Domain) > maxLength {
    		report.Domain = report.Domain[:maxLength]
		}


		// Insert into DB
		_, err = db.Exec("INSERT INTO reports (id, category, name, address, type, domain, timestamp, date) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
			report.ID, report.Category, report.Name, report.Address, report.Type, report.Domain, report.Timestamp, report.Date)
		if err != nil {
			log.Fatal(err)
		}
	})

	fmt.Printf("Visited: %s\n", url)
}

func getTotalReports() (int, error) {
	ctx, cancel := chromedp.NewContext(context.Background())
	defer cancel()

	var htmlContent string
	err := chromedp.Run(ctx,
		chromedp.Navigate("https://www.chainabuse.com/reports"),
		chromedp.WaitVisible(".create-ResultsSection__results-title"),
		chromedp.OuterHTML("html", &htmlContent),
	)
	if err != nil {
		return 0, err
	}

	doc, err := goquery.NewDocumentFromReader(strings.NewReader(htmlContent))
	if err != nil {
		return 0, err
	}

	var totalReports int
	doc.Find(".create-ResultsSection__results-title").Each(func(i int, e *goquery.Selection) {
		text := e.Text()
		words := strings.Fields(text)
		if len(words) > 0 {
			totalReports, _ = strconv.Atoi(words[0])
		}
	})

	return totalReports, nil
}

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

// Utility functions

func processNameField(s string) string {
	trimmed := strings.TrimSpace(s)
	var result []rune
	for _, r := range trimmed {
		if unicode.IsLetter(r) || unicode.IsSpace(r) {
			result = append(result, r)
		}
	}
	return string(result)
}

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
