package server

import "strings"

// matchTopic простая проверка wildcard: поддерживает + и # на уровне одного топика
func matchTopic(filter, topic string) bool {
	if filter == topic {
		return true
	}
	// Разбиваем на уровни
	filterParts := strings.Split(filter, "/")
	topicParts := strings.Split(topic, "/")
	// Если в фильтре есть #, он должен быть последним
	for i, f := range filterParts {
		if i >= len(topicParts) {
			if f == "#" {
				return true
			}
			return false
		}
		if f == "+" {
			continue
		}
		if f == "#" {
			return i == len(filterParts)-1 // # только в конце
		}
		if f != topicParts[i] {
			return false
		}
	}
	return len(filterParts) == len(topicParts)
}
