// Autogenerated by Thrift Compiler (0.11.0)
// DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING

package tutorial

import (
	"bytes"
	"context"
	"fmt"
	"git.apache.org/thrift.git/lib/go/thrift"
	"reflect"
	"thrift_p/gen-go/shared"
)

// (needed to ensure safety because of naive import list construction.)
var _ = thrift.ZERO
var _ = fmt.Printf
var _ = context.Background
var _ = reflect.DeepEqual
var _ = bytes.Equal

var _ = shared.GoUnusedProtection__

const INT32CONSTANT = 9853

var MAPCONSTANT map[string]string

func init() {
	MAPCONSTANT = map[string]string{
		"hello":     "world",
		"goodnight": "moon",
	}

}