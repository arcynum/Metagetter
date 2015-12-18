package main

import (
	"testing"
	"io/ioutil"
	"os"
)

func TestLoadMalformedConfiguration(t *testing.T) {
	_, err := loadConfiguration("testing/config_malformed.json");
	if err == nil {
		t.Fatal("Loaded malformed configuration data.", err)
	}
	return
}

func TestLoadShortConfiguration(t *testing.T) {
	_, err := loadConfiguration("testing/config_short.json");
	if err != nil {
		t.Fatal("Failed to load short configuration data", err)
	}
	return
}

func TestEmptyFolderSearch(t *testing.T) {
	directory, err := ioutil.TempDir("testing", "test_")
	if err != nil {
		t.Fatal("Could not create temp directory", err)
	}
	delta := findPreviousDelta(directory);
	
	// Remove the temp directory
	os.Remove(directory);

	if len(delta) != 0 {
		t.Fatal("A delta was found on an empty folder")
	}

	return
}

func TestSingleCorrectFolderSearch(t *testing.T) {
	directory, err := ioutil.TempDir("testing", "test_")
	if err != nil {
		t.Fatal("Could not create temp directory", err)
	}
	delta := findPreviousDelta(directory);
	
	// Remove the temp directory
	os.Remove(directory);

	if len(delta) != 0 {
		t.Fatal("A delta was found on an empty folder")
	}

	return
}

func TestSingleIncorrectFolderSearch(t *testing.T) {
	directory, err := ioutil.TempDir("testing", "test_")
	if err != nil {
		t.Fatal("Could not create temp directory", err)
	}

	sub_directory, err := ioutil.TempDir(directory, "test_")
	if err != nil {
		t.Fatal("Could not create temp directory", err)
	}

	delta := findPreviousDelta(directory);
	
	// Remove the temp directory
	os.Remove(sub_directory);
	os.Remove(directory);

	if len(delta) != 0 {
		t.Fatal("A delta was found on an empty folder")
	}

	return
}

/*
func TestStub(t *testing.T) {
	t.Error("This failed")
}
*/

/*
func TestCreateFolder(t *testing.T) {
	path := "results"
	err := os.Mkdir(path, 0777)
	if err != nil {
		t.Error("Could not create folder", path, "because", err.Error())
	}
}
*/