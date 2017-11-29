package util

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// FmtTask will format a task value from 'template' and 'dateHour'.
// It supports the following format values:
//
// {yyyy} (year - four digits: ie 2017)
// {yy}   (year - two digits: ie 17)
// {mm}   (month - two digits: ie 12)
// {dd}   (day - two digits: ie 13)
// {hh}   (hour - two digits: ie 00)
//
// Example:
//
// template == "{yyyy}-{mm}-{dd}T{hh}:00"
//
// could return: "2017-01-01T23:00"
//
// If that was the corresponding time.Time value of dateHour.
func FmtTask(template string, dateHour time.Time) string {

	// substitute year {yyyy}
	y := strconv.Itoa(dateHour.Year())
	s := strings.Replace(template, "{yyyy}", y, -1)

	// substitute year {yy}
	s = strings.Replace(s, "{yy}", y[2:], -1)

	// substitute month {mm}
	m := fmt.Sprintf("%02d", int(dateHour.Month()))
	s = strings.Replace(s, "{mm}", m, -1)

	// substitute day {dd}
	d := fmt.Sprintf("%02d", dateHour.Day())
	s = strings.Replace(s, "{dd}", d, -1)

	// substitute hour {hh}
	h := fmt.Sprintf("%02d", dateHour.Hour())
	s = strings.Replace(s, "{hh}", h, -1)

	return s
}
