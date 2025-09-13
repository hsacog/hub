package api

import (
	"hub/pkg/command"
	"net/http"

	"github.com/gin-gonic/gin"
)

type RestApiRunner struct {
	ch chan command.Command
	router *gin.Engine
}

func NewRestApiRunner(ch chan command.Command) command.CommandReceiver {
	router := gin.Default()
	api := router.Group("/api/v1")
	mkt := api.Group("/mkt")

	handler := &Handler{ch}

	mkt.POST("", handler.MktAdd)
	mkt.DELETE("", handler.MktRemove)

	return &RestApiRunner{ch, router}
}

func (r *RestApiRunner) Start() error {
	go func() {
		r.router.Run(":8080")
	}()
	return nil
}

func (r *RestApiRunner) Stop() error {
	return nil
}

type Handler struct {
	ch chan command.Command
}

func (handler *Handler) MktAdd(ctx *gin.Context) {
	var data Mkt
	if err := ctx.ShouldBindJSON(&data); err != nil {
		response := Response{
			Success: false,
			Message: "invalid json" + err.Error(),
		}
		ctx.JSON(http.StatusBadRequest, response)
		return
	}
	ctx.JSON(http.StatusOK, Response{
		Success: true,
	})
	
	var payload []any
	for _, p := range data.Pairs {
		payload = append(payload, command.MktPair{p.C1, p.C2})
	}
	handler.ch <- command.Command{
		Type: command.ADD_MKT,
		Payload: payload,
	}
}
func (handler *Handler) MktRemove(ctx *gin.Context) {
	var data Mkt
	if err := ctx.ShouldBindJSON(&data); err != nil {
		response := Response{
			Success: false,
			Message: "invalid json" + err.Error(),
		}
		ctx.JSON(http.StatusBadRequest, response)
		return
	}
	ctx.JSON(http.StatusOK, Response{
		Success: true,
	})

	var payload []any
	for _, p := range data.Pairs {
		payload = append(payload, command.MktPair{p.C1, p.C2})
	}
	handler.ch <- command.Command{
		Type: command.REMOVE_MKT,
		Payload: payload,
	}
}
