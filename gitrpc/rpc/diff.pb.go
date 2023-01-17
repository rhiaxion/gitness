// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.28.1
// 	protoc        v3.21.11
// source: diff.proto

package rpc

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type RawDiffRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Base *ReadRequest `protobuf:"bytes,1,opt,name=base,proto3" json:"base,omitempty"`
	// base_ref is left side of compare and can be branch, commit and tag
	BaseRef string `protobuf:"bytes,2,opt,name=base_ref,json=baseRef,proto3" json:"base_ref,omitempty"`
	// head_ref is right side of compare and can be branch, commit and tag
	HeadRef string `protobuf:"bytes,3,opt,name=head_ref,json=headRef,proto3" json:"head_ref,omitempty"`
	// merge_base used only in branch comparison, if merge_base is true
	// it will show diff from the commit where branch is created and head branch
	MergeBase bool `protobuf:"varint,4,opt,name=merge_base,json=mergeBase,proto3" json:"merge_base,omitempty"`
}

func (x *RawDiffRequest) Reset() {
	*x = RawDiffRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_diff_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *RawDiffRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RawDiffRequest) ProtoMessage() {}

func (x *RawDiffRequest) ProtoReflect() protoreflect.Message {
	mi := &file_diff_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use RawDiffRequest.ProtoReflect.Descriptor instead.
func (*RawDiffRequest) Descriptor() ([]byte, []int) {
	return file_diff_proto_rawDescGZIP(), []int{0}
}

func (x *RawDiffRequest) GetBase() *ReadRequest {
	if x != nil {
		return x.Base
	}
	return nil
}

func (x *RawDiffRequest) GetBaseRef() string {
	if x != nil {
		return x.BaseRef
	}
	return ""
}

func (x *RawDiffRequest) GetHeadRef() string {
	if x != nil {
		return x.HeadRef
	}
	return ""
}

func (x *RawDiffRequest) GetMergeBase() bool {
	if x != nil {
		return x.MergeBase
	}
	return false
}

type RawDiffResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Data []byte `protobuf:"bytes,1,opt,name=data,proto3" json:"data,omitempty"`
}

func (x *RawDiffResponse) Reset() {
	*x = RawDiffResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_diff_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *RawDiffResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RawDiffResponse) ProtoMessage() {}

func (x *RawDiffResponse) ProtoReflect() protoreflect.Message {
	mi := &file_diff_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use RawDiffResponse.ProtoReflect.Descriptor instead.
func (*RawDiffResponse) Descriptor() ([]byte, []int) {
	return file_diff_proto_rawDescGZIP(), []int{1}
}

func (x *RawDiffResponse) GetData() []byte {
	if x != nil {
		return x.Data
	}
	return nil
}

var File_diff_proto protoreflect.FileDescriptor

var file_diff_proto_rawDesc = []byte{
	0x0a, 0x0a, 0x64, 0x69, 0x66, 0x66, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x03, 0x72, 0x70,
	0x63, 0x1a, 0x0c, 0x73, 0x68, 0x61, 0x72, 0x65, 0x64, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22,
	0x8b, 0x01, 0x0a, 0x0e, 0x52, 0x61, 0x77, 0x44, 0x69, 0x66, 0x66, 0x52, 0x65, 0x71, 0x75, 0x65,
	0x73, 0x74, 0x12, 0x24, 0x0a, 0x04, 0x62, 0x61, 0x73, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b,
	0x32, 0x10, 0x2e, 0x72, 0x70, 0x63, 0x2e, 0x52, 0x65, 0x61, 0x64, 0x52, 0x65, 0x71, 0x75, 0x65,
	0x73, 0x74, 0x52, 0x04, 0x62, 0x61, 0x73, 0x65, 0x12, 0x19, 0x0a, 0x08, 0x62, 0x61, 0x73, 0x65,
	0x5f, 0x72, 0x65, 0x66, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x62, 0x61, 0x73, 0x65,
	0x52, 0x65, 0x66, 0x12, 0x19, 0x0a, 0x08, 0x68, 0x65, 0x61, 0x64, 0x5f, 0x72, 0x65, 0x66, 0x18,
	0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x68, 0x65, 0x61, 0x64, 0x52, 0x65, 0x66, 0x12, 0x1d,
	0x0a, 0x0a, 0x6d, 0x65, 0x72, 0x67, 0x65, 0x5f, 0x62, 0x61, 0x73, 0x65, 0x18, 0x04, 0x20, 0x01,
	0x28, 0x08, 0x52, 0x09, 0x6d, 0x65, 0x72, 0x67, 0x65, 0x42, 0x61, 0x73, 0x65, 0x22, 0x25, 0x0a,
	0x0f, 0x52, 0x61, 0x77, 0x44, 0x69, 0x66, 0x66, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65,
	0x12, 0x12, 0x0a, 0x04, 0x64, 0x61, 0x74, 0x61, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x04,
	0x64, 0x61, 0x74, 0x61, 0x32, 0x47, 0x0a, 0x0b, 0x44, 0x69, 0x66, 0x66, 0x53, 0x65, 0x72, 0x76,
	0x69, 0x63, 0x65, 0x12, 0x38, 0x0a, 0x07, 0x52, 0x61, 0x77, 0x44, 0x69, 0x66, 0x66, 0x12, 0x13,
	0x2e, 0x72, 0x70, 0x63, 0x2e, 0x52, 0x61, 0x77, 0x44, 0x69, 0x66, 0x66, 0x52, 0x65, 0x71, 0x75,
	0x65, 0x73, 0x74, 0x1a, 0x14, 0x2e, 0x72, 0x70, 0x63, 0x2e, 0x52, 0x61, 0x77, 0x44, 0x69, 0x66,
	0x66, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x30, 0x01, 0x42, 0x27, 0x5a,
	0x25, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x68, 0x61, 0x72, 0x6e,
	0x65, 0x73, 0x73, 0x2f, 0x67, 0x69, 0x74, 0x6e, 0x65, 0x73, 0x73, 0x2f, 0x67, 0x69, 0x74, 0x72,
	0x70, 0x63, 0x2f, 0x72, 0x70, 0x63, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_diff_proto_rawDescOnce sync.Once
	file_diff_proto_rawDescData = file_diff_proto_rawDesc
)

func file_diff_proto_rawDescGZIP() []byte {
	file_diff_proto_rawDescOnce.Do(func() {
		file_diff_proto_rawDescData = protoimpl.X.CompressGZIP(file_diff_proto_rawDescData)
	})
	return file_diff_proto_rawDescData
}

var file_diff_proto_msgTypes = make([]protoimpl.MessageInfo, 2)
var file_diff_proto_goTypes = []interface{}{
	(*RawDiffRequest)(nil),  // 0: rpc.RawDiffRequest
	(*RawDiffResponse)(nil), // 1: rpc.RawDiffResponse
	(*ReadRequest)(nil),     // 2: rpc.ReadRequest
}
var file_diff_proto_depIdxs = []int32{
	2, // 0: rpc.RawDiffRequest.base:type_name -> rpc.ReadRequest
	0, // 1: rpc.DiffService.RawDiff:input_type -> rpc.RawDiffRequest
	1, // 2: rpc.DiffService.RawDiff:output_type -> rpc.RawDiffResponse
	2, // [2:3] is the sub-list for method output_type
	1, // [1:2] is the sub-list for method input_type
	1, // [1:1] is the sub-list for extension type_name
	1, // [1:1] is the sub-list for extension extendee
	0, // [0:1] is the sub-list for field type_name
}

func init() { file_diff_proto_init() }
func file_diff_proto_init() {
	if File_diff_proto != nil {
		return
	}
	file_shared_proto_init()
	if !protoimpl.UnsafeEnabled {
		file_diff_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*RawDiffRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_diff_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*RawDiffResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_diff_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   2,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_diff_proto_goTypes,
		DependencyIndexes: file_diff_proto_depIdxs,
		MessageInfos:      file_diff_proto_msgTypes,
	}.Build()
	File_diff_proto = out.File
	file_diff_proto_rawDesc = nil
	file_diff_proto_goTypes = nil
	file_diff_proto_depIdxs = nil
}