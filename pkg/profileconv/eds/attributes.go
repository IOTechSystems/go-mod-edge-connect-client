// Copyright (C) 2026 IOTech Ltd

package eds

// EtherNet/IP device-resource attribute keys and type values, per the XRT
// EtherNet/IP device-service spec:
// https://docs.iotechsys.com/edge-xrt22/device-service-components/ethernet-ip-device-service-component.html
//
// A resource's kind is told apart by its "type" attribute (or its absence for
// explicit messaging):
//
//	implicit I/O : type O2T / T2O    + offsetBytes / offsetBits / bitLength
//	settings     : type *Settings    + assemblyID / size / includeHeader32bit
//	explicit     : (no type)         + objClass / instID / attrID
//
// The spec also defines a logixTag kind (type "logixTag" + tagName / arraySize)
// for Allen-Bradley Logix tag access. It is intentionally NOT produced here: a
// Logix tag is defined by the PLC program, not the EDS, so it has no source in
// an EDS file this converter reads.
const (
	// attribute keys
	attrType               = "type"
	attrOffsetBytes        = "offsetBytes"
	attrOffsetBits         = "offsetBits"
	attrBitLength          = "bitLength"
	attrAssemblyID         = "assemblyID"
	attrSize               = "size"
	attrIncludeHeader32bit = "includeHeader32bit"
	attrObjClass           = "objClass"
	attrInstID             = "instID"
	attrAttrID             = "attrID"

	// type attribute values
	typeO2T            = "O2T" // originator-to-target: written (output)
	typeT2O            = "T2O" // target-to-originator: read (input)
	typeO2TSettings    = "O2TSettings"
	typeT2OSettings    = "T2OSettings"
	typeConfigSettings = "ConfigSettings"
)
