// Copyright (C) 2023 IOTech Ltd

package xrt

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/errors"
)

func FetchXRTResponse(ctx context.Context, requestId string, requestMap RequestMap, responseTimeout time.Duration) ([]byte, errors.EdgeX) {
	resChan, ok := requestMap.Get(requestId)
	if !ok {
		return nil, errors.NewCommonEdgeX(errors.KindServerError, fmt.Sprintf("the corresponding ResponseChan not found by requestId %s", requestId), nil)
	}
	defer func() {
		close(resChan)
		requestMap.Delete(requestId)
	}()

	timeout := time.After(responseTimeout)
	select {
	case <-ctx.Done():
		return nil, nil
	case <-timeout:
		return nil, errors.NewCommonEdgeX(errors.KindServerError, "timed out fetching command response", nil)
	case commandResponse := <-resChan:
		return commandResponse, nil
	}
}

// FetchXRTResWithSubTimeout subscribe multiple messages of the same requestId for the given subscribe timeout, and the result will be appended in the response slice
// After the subscribe timeout, the response slice pointer will be returned
func FetchXRTResWithSubTimeout(ctx context.Context, requestId string, requestMap RequestMap, subscribeTimeout time.Duration, response any) errors.EdgeX {
	resChan, ok := requestMap.Get(requestId)
	if !ok {
		return errors.NewCommonEdgeX(errors.KindServerError, fmt.Sprintf("the corresponding ResponseChan not found by requestId %s", requestId), nil)
	}

	defer func() {
		close(resChan)
		requestMap.Delete(requestId)
	}()

	subTimeout := time.After(subscribeTimeout)

	if reflect.ValueOf(response).Kind() != reflect.Ptr || reflect.ValueOf(response).Elem().Kind() != reflect.Slice {
		return errors.NewCommonEdgeX(errors.KindServerError, "the response type must be a pointer to a slice", nil)
	}

	// get the slice type from the pointer
	sliceType := reflect.TypeOf(response).Elem()
	respSlice := reflect.MakeSlice(sliceType, 0, 0)

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-subTimeout:
			// set the respSlice slice back to the response interface
			reflect.ValueOf(response).Elem().Set(respSlice)
			return nil
		case commandResponse := <-resChan:
			// create a new element Value
			element := reflect.New(sliceType.Elem())
			tmp := element.Interface()
			// unmarshal the commandResponse bytes to the tmp pointer
			err := json.Unmarshal(commandResponse, tmp)
			if err != nil {
				return errors.NewCommonEdgeX(errors.KindServerError,
					fmt.Sprintf("failed to unmarshal response bytes from message bus to element: %v", element.Type()), nil)
			}

			// append the element value to respSlice
			respSlice = reflect.Append(respSlice, element.Elem())
		}
	}
}
