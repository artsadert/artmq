package server

import "strings"

// matchTopic reports whether topic matches an MQTT subscription filter.
//
// Wildcards: '+' matches a single level; '#' matches zero or more trailing
// levels and is valid only as the final segment. The implementation walks
// both strings in place via strings.IndexByte('/') and never allocates.
func matchTopic(filter, topic string) bool {
	if filter == topic {
		return true
	}
	for {
		// Pull the next filter segment.
		fSlash := strings.IndexByte(filter, '/')
		var fSeg string
		if fSlash < 0 {
			fSeg = filter
		} else {
			fSeg = filter[:fSlash]
		}

		// '#' must be the final filter segment and matches everything left,
		// including the empty tail (so "a/#" matches "a"). This mirrors
		// MQTT 5 §4.7.1.2.
		if fSeg == "#" {
			return fSlash < 0
		}

		// Pull the next topic segment.
		tSlash := strings.IndexByte(topic, '/')
		var tSeg string
		if tSlash < 0 {
			if topic == "" {
				// filter has more segments but topic is exhausted
				return false
			}
			tSeg = topic
		} else {
			tSeg = topic[:tSlash]
		}

		if fSeg != "+" && fSeg != tSeg {
			return false
		}

		// Advance.
		if fSlash < 0 && tSlash < 0 {
			return true
		}
		if fSlash < 0 || tSlash < 0 {
			// One ran out before the other (and remaining side is not '#').
			return false
		}
		filter = filter[fSlash+1:]
		topic = topic[tSlash+1:]
	}
}
