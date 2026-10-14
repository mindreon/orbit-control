package jcs

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"testing"
)

func TestRFC8785(t *testing.T) {
	cases := []struct{ input, want string }{
		{`{"numbers":[333333333.33333329,1E30,4.50,2e-3,0.000000000000000000000000001],"string":"\u20ac$\u000F\nA'B\"\\\"/","literals":[null,true,false]}`, `{"literals":[null,true,false],"numbers":[333333333.3333333,1e+30,4.5,0.002,1e-27],"string":"€$\u000f\nA'B\"\\\"/"}`},
		{`{"\u20ac":"Euro Sign","\r":"Carriage Return","\ufb33":"Hebrew Letter Dalet With Dagesh","1":"One","\ud83d\ude00":"Emoji: Grinning Face","\u0080":"Control","\u00f6":"Latin Small Letter O With Diaeresis"}`, "{\"\\r\":\"Carriage Return\",\"1\":\"One\",\"\u0080\":\"Control\",\"ö\":\"Latin Small Letter O With Diaeresis\",\"€\":\"Euro Sign\",\"😀\":\"Emoji: Grinning Face\",\"דּ\":\"Hebrew Letter Dalet With Dagesh\"}"},
		{`["<>&","\u2028","\u2029",-0]`, "[\"<>&\",\"\u2028\",\"\u2029\",0]"},
	}
	for _, c := range cases {
		got, err := Canonicalize([]byte(c.input))
		if err != nil || string(got) != c.want {
			t.Errorf("got %s, want %s: %v", got, c.want, err)
		}
	}
	for _, input := range []string{`{"a":1,"\u0061":2}`, `"\ud800"`, `"\udfff"`, `1e999`, `{} {}`, "\"\xff\""} {
		if _, err := Canonicalize([]byte(input)); err == nil {
			t.Errorf("accepted %q", input)
		}
	}
}

func TestRFC8785AppendixB(t *testing.T) {
	cases := [][2]string{
		{"0000000000000000", "0"}, {"8000000000000000", "0"},
		{"0000000000000001", "5e-324"}, {"8000000000000001", "-5e-324"},
		{"7fefffffffffffff", "1.7976931348623157e+308"}, {"ffefffffffffffff", "-1.7976931348623157e+308"},
		{"4340000000000000", "9007199254740992"}, {"c340000000000000", "-9007199254740992"},
		{"4430000000000000", "295147905179352830000"},
		{"44b52d02c7e14af5", "9.999999999999997e+22"}, {"44b52d02c7e14af6", "1e+23"}, {"44b52d02c7e14af7", "1.0000000000000001e+23"},
		{"444b1ae4d6e2ef4e", "999999999999999700000"}, {"444b1ae4d6e2ef4f", "999999999999999900000"}, {"444b1ae4d6e2ef50", "1e+21"},
		{"3eb0c6f7a0b5ed8c", "9.999999999999997e-7"}, {"3eb0c6f7a0b5ed8d", "0.000001"},
		{"41b3de4355555553", "333333333.3333332"}, {"41b3de4355555554", "333333333.33333325"}, {"41b3de4355555555", "333333333.3333333"}, {"41b3de4355555556", "333333333.3333334"}, {"41b3de4355555557", "333333333.33333343"},
		{"becbf647612f3696", "-0.0000033333333333333333"}, {"43143ff3c1cb0959", "1424953923781206.2"},
	}
	for _, c := range cases {
		t.Run(c[0], func(t *testing.T) {
			b, _ := hex.DecodeString(c[0])
			v := math.Float64frombits(binary.BigEndian.Uint64(b))
			got, err := Canonicalize([]byte(strconv.FormatFloat(v, 'g', -1, 64)))
			if err != nil || string(got) != c[1] {
				t.Fatalf("%s want %s: %v", got, c[1], err)
			}
		})
	}
	for _, bits := range []uint64{0x7fffffffffffffff, 0x7ff0000000000000} {
		if _, err := Marshal(math.Float64frombits(bits)); err == nil {
			t.Fatal(fmt.Sprintf("accepted %x", bits))
		}
	}
}
