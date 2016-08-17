package main

import "os"
import "fmt"
import "mapreduce"
import "container/list"

import (
	"log"
	"unicode"
	"strings"
	"strconv"
)

// Separates value into individual words and counts the number of times each
// word is present. The final counts are returned as a list of (key, value)
// tuples where each key is a unique word and the corresponding value is the
// count of that word.
func Map(value string) *list.List {
	// (1) Split up the string into words, discarding any punctuation
	separator := func(r rune) bool {
		return !unicode.IsLetter(r) }
	words := strings.FieldsFunc(value, separator)

	// (!) Perform a local word count to reduce the amount of data being passed
	// to Reduce
	m := make(map[string]int)
	for _, word := range words {
		if count, ok := m[word]; ok {
		    m[word] = count + 1
		} else {
		    m[word] = 1
		}
	}

	// (2) Add each word to the list with a mapreduce.KeyValue struct
	result := list.New()
	for word, count := range m {
		result.PushBack(mapreduce.KeyValue{word, strconv.Itoa(count)})
	}

	return result
}

// Combines the count value from every tuple returned from Map that has the same
// word as its key. The sum is returned as a string.
func Reduce(key string, values *list.List) string {
	// (1) Reduce all of the values in the values list
	sum := 0
	for value := values.Front(); value != nil; value = value.Next() {
		if count, err := strconv.Atoi(value.Value.(string)); err != nil {
			log.Fatal(err)
		} else {
			sum += count
		}
	}

	// (2) Return the reduced/summed up values as a string
	return strconv.Itoa(sum)
}

// Can be run in 3 ways:
// 1) Sequential (e.g., go run wc.go master x.txt sequential)
// 2) Master (e.g., go run wc.go master x.txt localhost:7777)
// 3) Worker (e.g., go run wc.go worker localhost:7777 localhost:7778 &)
func main() {
	if len(os.Args) != 4 {
		fmt.Printf("%s: see usage comments in file\n", os.Args[0])
	} else if os.Args[1] == "master" {
		if os.Args[3] == "sequential" {
			mapreduce.RunSingle(5, 3, os.Args[2], Map, Reduce)
		} else {
			mr := mapreduce.MakeMapReduce(5, 3, os.Args[2], os.Args[3])
			// Wait until MR is done
			<-mr.DoneChannel
		}
	} else {
		mapreduce.RunWorker(os.Args[2], os.Args[3], Map, Reduce, 100)
	}
}
