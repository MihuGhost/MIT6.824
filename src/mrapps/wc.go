package main

//
// a word-count application "plugin" for MapReduce.
//
// go build -buildmode=plugin wc.go
//

import "6.824/mr"
import "unicode"
import "strings"
import "strconv"

//Map修改后，返回单词切片
func Map(filename string, contents string) []string {
	//切分函数
	ff := func(r rune) bool { return !unicode.IsLetter(r) }

	//单词遍历放入kv  value都为1
	words := strings.FieldsFunc(contents, ff)

	strSlice := []string{}
	for _, w := range words {
		strSlice = append(strSlice, w)
	}
	return strSlice
}

func Reduce(key string,value int) string {
	return strconv.Itoa(value)
}