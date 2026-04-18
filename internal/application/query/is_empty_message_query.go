package query

type IsEmptyMessageQuery struct {
	TopicName string `json:"topicName"`
}

type IsEmptyMessageQueryResult struct {
	IsEmpty bool
}
