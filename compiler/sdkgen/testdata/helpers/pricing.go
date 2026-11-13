package helpers

import "strings"

func (i Item) Total() int { return i.Qty * i.Cents }

func orderTotal(items []Item) int {
	total := 0
	for _, item := range items {
		total += item.Total()
	}
	return total
}

func describe(orderID string, cents int) string {
	return strings.ToUpper(orderID) + " " + money(cents)
}
