package handler

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/psds-microservice/search-service/internal/service"
	"github.com/psds-microservice/search-service/internal/validator"
)

type SearchHandler struct {
	svc       *service.SearchService
	validator *validator.Validator
}

func NewSearchHandler(svc *service.SearchService) *SearchHandler {
	return &SearchHandler{
		svc:       svc,
		validator: validator.New(),
	}
}

// Search GET /search?q=...&type=tickets|sessions|operators|all&limit=20
func (h *SearchHandler) Search(c *gin.Context) {
	q := c.Query("q")
	typeFilter := c.Query("type")
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "20"))
	
	if err := h.validator.ValidateSearchQuery(q, typeFilter, limit); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	
	result, err := h.svc.Search(c.Request.Context(), q, typeFilter, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, result)
}

// IndexTicket POST /search/index/ticket
func (h *SearchHandler) IndexTicket(c *gin.Context) {
	var in service.IndexTicketInput
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body"})
		return
	}
	if err := h.validator.ValidateIndexTicketInput(in.TicketID, in.SessionID); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := h.svc.IndexTicket(c.Request.Context(), &in); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

// IndexSession POST /search/index/session
func (h *SearchHandler) IndexSession(c *gin.Context) {
	var in service.IndexSessionInput
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body"})
		return
	}
	if err := h.validator.ValidateIndexSessionInput(in.SessionID); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := h.svc.IndexSession(c.Request.Context(), &in); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

// IndexOperator POST /search/index/operator
func (h *SearchHandler) IndexOperator(c *gin.Context) {
	var in service.IndexOperatorInput
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body"})
		return
	}
	if err := h.validator.ValidateIndexOperatorInput(in.UserID); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := h.svc.IndexOperator(c.Request.Context(), &in); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}
