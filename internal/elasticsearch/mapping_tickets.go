package elasticsearch

// TicketsMapping возвращает маппинг индекса тикетов для Elasticsearch.
// Поля: ticket_id (long), session_id/client_id/operator_id/status (keyword), subject (text+keyword), notes (text).
func TicketsMapping() map[string]interface{} {
	return map[string]interface{}{
		"mappings": map[string]interface{}{
			"properties": map[string]interface{}{
				"ticket_id":   map[string]interface{}{"type": "long"},
				"session_id":  map[string]interface{}{"type": "keyword"},
				"client_id":   map[string]interface{}{"type": "keyword"},
				"operator_id": map[string]interface{}{"type": "keyword"},
				"subject": map[string]interface{}{
					"type": "text",
					"fields": map[string]interface{}{
						"keyword": map[string]interface{}{"type": "keyword", "ignore_above": 512},
					},
				},
				"notes":  map[string]interface{}{"type": "text"},
				"status": map[string]interface{}{"type": "keyword"},
			},
		},
	}
}
