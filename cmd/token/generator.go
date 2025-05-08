package main

import (
	"log"
	"os"

	"golang.org/x/oauth2/google"
	"google.golang.org/api/sheets/v4"
	"google.golang.org/api/slides/v1"
	mygoogle "tron-tracker/google"
)

func main() {
	// Load OAuth2 client credentials
	b, err := os.ReadFile("/Users/asuka/Works/Tests/TVM/tracker/credentials.json")
	if err != nil {
		log.Fatalf("Unable to read client secret file: %v", err)
	}

	// Parse client credentials
	config, err := google.ConfigFromJSON(b, slides.PresentationsScope, sheets.SpreadsheetsScope)
	if err != nil {
		log.Fatalf("Unable to parse client secret file to config: %v", err)
	}

	// Get a token from web authorization
	token := mygoogle.GetTokenFromWeb(config)

	// Save the token to a file
	mygoogle.SaveToken("/Users/asuka/Works/Tests/TVM/tracker/token.json", token)
}
