package router

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/psds-microservice/helpy/paths"
	"github.com/psds-microservice/search-service/api"
	"github.com/psds-microservice/search-service/internal/handler"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
)

func New(searchHandler *handler.SearchHandler) http.Handler {
	r := gin.New()
	r.Use(gin.Recovery())
	r.GET(paths.PathHealth, handler.Health)
	r.GET(paths.PathReady, handler.Ready)
	r.GET(paths.PathSwagger, func(c *gin.Context) { c.Redirect(http.StatusFound, paths.PathSwagger+"/") })
	r.GET(paths.PathSwagger+"/*any", func(c *gin.Context) {
		if strings.TrimPrefix(c.Param("any"), "/") == "openapi.json" {
			c.Data(http.StatusOK, "application/json", api.OpenAPISpec)
			return
		}
		if strings.TrimPrefix(c.Param("any"), "/") == "" {
			c.Request.URL.Path = paths.PathSwagger + "/index.html"
			c.Request.RequestURI = paths.PathSwagger + "/index.html"
		}
		ginSwagger.WrapHandler(swaggerFiles.Handler, ginSwagger.URL("/swagger/openapi.json"))(c)
	})
	r.GET("/search", searchHandler.Search)
	r.POST("/search/index/ticket", searchHandler.IndexTicket)
	r.POST("/search/index/session", searchHandler.IndexSession)
	r.POST("/search/index/operator", searchHandler.IndexOperator)
	return r
}
