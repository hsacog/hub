package api

import (
	"hub/pkg/command"
	"log"
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
	handler := &Handler{ch}

	ctl := api.Group("/ctl")
	ctl.POST("", handler.Control)
	
	sub := api.Group("/sub")
	sub.POST("", handler.Subscribe)
	sub.DELETE("", handler.UnSubscribe)

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

func (handler *Handler) Control(ctx *gin.Context) {
	var data Ctl 	
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
	log.Println("command", data)
	if data.Cmd == "RESET" {
		handler.ch <- command.Command{
			Type: command.RESET,
		}
	}		
}

func (handler *Handler) Subscribe(ctx *gin.Context) {
	var data Sub
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
func (handler *Handler) UnSubscribe(ctx *gin.Context) {
	var data Sub
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
