// Copyright (C) 2026 IOTech Ltd

package eds

import (
	"fmt"
	"sort"
	"strings"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/common"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/errors"
)

// mapExplicit builds explicit-messaging resources from params that
// carry a Link Path — a CIP object EPATH such as "20 01 24 01 30 06" giving
// objClass/instID/attrID. An explicit resource has no type attribute.
func (x *extracted) mapExplicit(names *nameSet) ([]dtos.DeviceResource, errors.EdgeX) {
	var resources []dtos.DeviceResource
	// Iterate params in key order so output is deterministic (map iteration is not).
	paramKeys := make([]string, 0, len(x.params))
	for k := range x.params {
		paramKeys = append(paramKeys, k)
	}
	sort.Strings(paramKeys)

	for _, k := range paramKeys {
		p := x.params[k]
		if strings.TrimSpace(p.linkPath) == "" {
			continue
		}
		attrs, err := explicitAttrsFromLinkPath(p.linkPath)
		if err != nil {
			return nil, errors.NewCommonEdgeX(errors.KindContractInvalid, fmt.Sprintf("explicit param %q", k), err)
		}
		valueType, verr := valueTypeForCIP(p.dataType)
		if verr != nil {
			return nil, errors.NewCommonEdgeX(errors.KindContractInvalid, fmt.Sprintf("explicit param %q", k), verr)
		}
		resources = append(resources, dtos.DeviceResource{
			Name:        names.unique(p.name, "explicit"),
			Description: p.help,
			Attributes:  attrs,
			Properties:  paramProperties(p, valueType, common.ReadWrite_R),
		})
	}
	return resources, nil
}

// explicitAttrsFromLinkPath parses a param Link Path EPATH into explicit-messaging
// attributes. "20 01 24 01 30 06" -> objClass=1, instID=1, attrID=6. The class
// and instance segments are required; the attribute segment is optional —
// omitting it reads the whole instance (Get Attributes All). Uses the shared
// parseEPATH walker; a duplicate class/instance/attribute segment is rejected here.
func explicitAttrsFromLinkPath(path string) (map[string]any, errors.EdgeX) {
	segs, err := parseEPATH(path)
	if err != nil {
		return nil, err
	}
	attrs := map[string]any{}
	key := map[uint64]string{segClass: attrObjClass, segInstance: attrInstID, segAttribute: attrAttrID}
	for _, s := range segs {
		k, ok := key[s.logicalType]
		if !ok {
			return nil, errors.NewCommonEdgeX(errors.KindContractInvalid, fmt.Sprintf("unexpected Link Path segment 0x%02X in %q", s.logicalType, path), nil)
		}
		if _, dup := attrs[k]; dup {
			return nil, errors.NewCommonEdgeX(errors.KindContractInvalid, fmt.Sprintf("duplicate %s segment in Link Path %q", k, path), nil)
		}
		attrs[k] = s.value
	}
	if _, ok := attrs[attrObjClass]; !ok {
		return nil, errors.NewCommonEdgeX(errors.KindContractInvalid, fmt.Sprintf("Link Path %q missing class segment", path), nil)
	}
	if _, ok := attrs[attrInstID]; !ok {
		return nil, errors.NewCommonEdgeX(errors.KindContractInvalid, fmt.Sprintf("Link Path %q missing instance segment", path), nil)
	}
	return attrs, nil
}
