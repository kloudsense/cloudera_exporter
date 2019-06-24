/*
 *
 * title           :json_cloudera_api_timeseries_queries_parser.go
 * description     :File with specific functions to parse JSONs files
 * author		       :Alejandro Villegas Lopez (avillegas@keedio.com)
 * date            :2019/02/05
 * version         :1.0
 * notes           :Module 
 * module          :Keedio Cloudera Exporter (KCE)
 *
 */
package json_parser

/* 
 * Dependencies
 */
import (
  // Go Default libraries
  "fmt"
  "strings"

  // Go JSON parsing libraries
	"github.com/tidwall/gjson"
)

// Returns the JSON parsed as an gjson.Result Obtject
func Parse_json_response(json string) gjson.Result {
  return gjson.Parse(json)
}

// Given a parsed JSON and an element within it, it returns that element as a
// string or "" if it did not find the specified element
func Get_json_field(json gjson.Result, item string) string {
  return json.Get(item).String()
}

// Given a parsed JSON and an element within it, it returns that element as a
// list or "" if it did not find the specified element
func Get_json_array(json gjson.Result, item string) []gjson.Result {
  return json.Get(item).Array()
}

// Compose the complete URL connection to the Cloudera API in HTTP format 
func Encode_tsquery_to_http(tsquery string) string {
  return fmt.Sprintf("query=%s", Encode_http_symbols(tsquery))
}

// Find and replace special symbols to the HTTP encoding format
func Encode_http_symbols(s string) string {
  return strings.Replace(s, " ", "+", -1)
}
