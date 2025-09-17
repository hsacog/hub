package kafka

import (
	. "hub/pkg/pipeline"
)

func ProducePipeline() *Pipeline[*PlData, *PlData] {
	return NewPipeline(1000, func (data *PlData) *PlData {
		return data	
	})
}

