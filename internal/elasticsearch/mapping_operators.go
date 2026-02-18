package elasticsearch

// OperatorsMapping возвращает маппинг индекса операторов для Elasticsearch.
// Поля: user_id, region, role (keyword), display_name (text + keyword subfield).
func OperatorsMapping() map[string]interface{} {
	return map[string]interface{}{
		"mappings": map[string]interface{}{
			"properties": map[string]interface{}{
				"user_id": map[string]interface{}{"type": "keyword"},
				"display_name": map[string]interface{}{
					"type": "text",
					"fields": map[string]interface{}{
						"keyword": map[string]interface{}{"type": "keyword", "ignore_above": 256},
					},
				},
				"region": map[string]interface{}{"type": "keyword"},
				"role":   map[string]interface{}{"type": "keyword"},
			},
		},
	}
}
