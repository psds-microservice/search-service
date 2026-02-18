package elasticsearch

// SessionsMapping возвращает маппинг индекса сессий для Elasticsearch.
// Поля: session_id, client_id, pin, status (keyword).
func SessionsMapping() map[string]interface{} {
	return map[string]interface{}{
		"mappings": map[string]interface{}{
			"properties": map[string]interface{}{
				"session_id": map[string]interface{}{"type": "keyword"},
				"client_id":  map[string]interface{}{"type": "keyword"},
				"pin":        map[string]interface{}{"type": "keyword"},
				"status":     map[string]interface{}{"type": "keyword"},
			},
		},
	}
}
