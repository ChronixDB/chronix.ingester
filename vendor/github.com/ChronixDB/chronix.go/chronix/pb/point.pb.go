// Code generated by protoc-gen-go.
// source: MetricPoint.proto
// DO NOT EDIT!

/*
Package MetricPoint is a generated protocol buffer package.

It is generated from these files:
	MetricPoint.proto

It has these top-level messages:
	Point
	Points
*/
package pb

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion2 // please upgrade the proto package

// Our point
type Point struct {
	// The delta as long
	Tlong *uint64 `protobuf:"varint,1,opt,name=tlong" json:"tlong,omitempty"`
	// Perhaps we can store the delta as int
	Tint *uint32 `protobuf:"varint,2,opt,name=tint" json:"tint,omitempty"`
	// timestamp base deltas
	TlongBP *uint64 `protobuf:"varint,3,opt,name=tlongBP" json:"tlongBP,omitempty"`
	TintBP  *uint32 `protobuf:"varint,4,opt,name=tintBP" json:"tintBP,omitempty"`
	// Value
	V *float64 `protobuf:"fixed64,5,opt,name=v" json:"v,omitempty"`
	// Or the index of the value
	VIndex           *uint32 `protobuf:"varint,6,opt,name=vIndex" json:"vIndex,omitempty"`
	XXX_unrecognized []byte  `json:"-"`
}

func (m *Point) Reset()                    { *m = Point{} }
func (m *Point) String() string            { return proto.CompactTextString(m) }
func (*Point) ProtoMessage()               {}
func (*Point) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{0} }

func (m *Point) GetTlong() uint64 {
	if m != nil && m.Tlong != nil {
		return *m.Tlong
	}
	return 0
}

func (m *Point) GetTint() uint32 {
	if m != nil && m.Tint != nil {
		return *m.Tint
	}
	return 0
}

func (m *Point) GetTlongBP() uint64 {
	if m != nil && m.TlongBP != nil {
		return *m.TlongBP
	}
	return 0
}

func (m *Point) GetTintBP() uint32 {
	if m != nil && m.TintBP != nil {
		return *m.TintBP
	}
	return 0
}

func (m *Point) GetV() float64 {
	if m != nil && m.V != nil {
		return *m.V
	}
	return 0
}

func (m *Point) GetVIndex() uint32 {
	if m != nil && m.VIndex != nil {
		return *m.VIndex
	}
	return 0
}

// The data of a time series is a list of points
type Points struct {
	// The list of points
	P []*Point `protobuf:"bytes,1,rep,name=p" json:"p,omitempty"`
	// the used ddc threshold
	Ddc              *uint32 `protobuf:"varint,2,opt,name=ddc" json:"ddc,omitempty"`
	XXX_unrecognized []byte  `json:"-"`
}

func (m *Points) Reset()                    { *m = Points{} }
func (m *Points) String() string            { return proto.CompactTextString(m) }
func (*Points) ProtoMessage()               {}
func (*Points) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{1} }

func (m *Points) GetP() []*Point {
	if m != nil {
		return m.P
	}
	return nil
}

func (m *Points) GetDdc() uint32 {
	if m != nil && m.Ddc != nil {
		return *m.Ddc
	}
	return 0
}

func init() {
	proto.RegisterType((*Point)(nil), "Point")
	proto.RegisterType((*Points)(nil), "Points")
}

func init() { proto.RegisterFile("MetricPoint.proto", fileDescriptor0) }

var fileDescriptor0 = []byte{
	// 223 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x09, 0x6e, 0x88, 0x02, 0xff, 0x34, 0x8f, 0xcd, 0x4a, 0xc5, 0x30,
	0x10, 0x85, 0x19, 0xfb, 0x23, 0x8c, 0x0a, 0x1a, 0x54, 0xb2, 0x94, 0xbb, 0xba, 0xb8, 0x08, 0xe2,
	0x23, 0x14, 0x04, 0x5d, 0x08, 0x97, 0xbe, 0x41, 0x49, 0xe6, 0x5e, 0x03, 0x25, 0xa9, 0xd3, 0x18,
	0x8b, 0x6f, 0xe0, 0x5b, 0x9b, 0xa4, 0x75, 0x37, 0xdf, 0x39, 0xdf, 0xe2, 0x0c, 0xde, 0xbc, 0x53,
	0x60, 0xab, 0x0f, 0xde, 0xba, 0xa0, 0x26, 0xf6, 0xc1, 0xef, 0x7e, 0x01, 0x9b, 0xc2, 0xe2, 0x16,
	0x9b, 0x30, 0x7a, 0x77, 0x92, 0xf0, 0x00, 0xfb, 0xba, 0x5f, 0x41, 0x08, 0xac, 0x43, 0x6a, 0xe5,
	0x59, 0x0a, 0xaf, 0xfa, 0x72, 0x0b, 0x89, 0xe7, 0xa5, 0xec, 0x0e, 0xb2, 0x2a, 0xee, 0x3f, 0x8a,
	0x7b, 0x6c, 0xb3, 0x91, 0x8a, 0xba, 0xf8, 0x1b, 0x89, 0x4b, 0x84, 0x28, 0x9b, 0x14, 0x41, 0x0f,
	0x31, 0x5b, 0xf1, 0xcd, 0x19, 0x5a, 0x64, 0xbb, 0x5a, 0x2b, 0xed, 0x9e, 0xb0, 0x2d, 0x53, 0xe6,
	0xb4, 0x05, 0xa6, 0xb4, 0xa3, 0xda, 0x5f, 0x3c, 0xb7, 0xaa, 0x64, 0x3d, 0x4c, 0xe2, 0x1a, 0x2b,
	0x63, 0xf4, 0x36, 0x25, 0x9f, 0xdd, 0x0b, 0x3e, 0x1a, 0x52, 0x9f, 0xc3, 0xf7, 0xc0, 0xa4, 0xf4,
	0x07, 0x7b, 0x67, 0x17, 0xa5, 0xbd, 0x8b, 0xc4, 0x81, 0x58, 0xcd, 0xc4, 0x76, 0x18, 0xed, 0x4f,
	0x3a, 0x4f, 0xe4, 0xba, 0xbb, 0xed, 0xfd, 0xfc, 0xb8, 0xf6, 0x63, 0xf7, 0x75, 0x3c, 0x12, 0xcf,
	0xaf, 0xf0, 0x17, 0x00, 0x00, 0xff, 0xff, 0xe5, 0xc1, 0x38, 0x07, 0x18, 0x01, 0x00, 0x00,
}