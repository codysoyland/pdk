// Copyright 2017 Pilosa Corp.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions
// are met:
//
// 1. Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//
// 2. Redistributions in binary form must reproduce the above copyright
// notice, this list of conditions and the following disclaimer in the
// documentation and/or other materials provided with the distribution.
//
// 3. Neither the name of the copyright holder nor the names of its
// contributors may be used to endorse or promote products derived
// from this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND
// CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
// MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
// CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
// BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
// WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
// NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
// DAMAGE.

package pdk_test

import (
	"testing"
	"time"

	gopilosa "github.com/pilosa/go-pilosa"
	"github.com/pilosa/pdk"
	ptest "github.com/pilosa/pilosa/test"
)

func TestSetupPilosa(t *testing.T) {
	s := ptest.MustRunMainWithCluster(t, 2)
	hosts := []string{}
	for _, com := range s {
		hosts = append(hosts, "http://"+com.Server.Addr().String())
	}

	frames := []pdk.FrameSpec{
		{
			Name:      "frame1",
			CacheType: gopilosa.CacheTypeRanked,
			CacheSize: 17,
		},
		{
			Name:      "frame2",
			CacheType: gopilosa.CacheTypeLRU,
			CacheSize: 19,
		},
		{
			Name: "frame3",
			Fields: []pdk.FieldSpec{
				{
					Name: "field1",
					Min:  0,
					Max:  3999999,
				},
				{
					Name: "field2",
					Min:  10000,
					Max:  20000,
				},
			},
		},
		{
			Name:        "frametime",
			CacheType:   gopilosa.CacheTypeRanked,
			CacheSize:   100,
			TimeQuantum: gopilosa.TimeQuantumYearMonthDay,
		},
	}

	indexer, err := pdk.SetupPilosa(hosts, "newindex", frames, 2)
	if err != nil {
		t.Fatalf("SetupPilosa: %v", err)
	}

	indexer.AddBit("frame1", 0, 0)
	indexer.AddValue("frame3", "field1", 0, 97)
	indexer.AddBitTimestamp("frametime", 0, 0, time.Date(2018, time.February, 22, 9, 0, 0, 0, time.UTC))
	indexer.AddBitTimestamp("frametime", 2, 0, time.Date(2018, time.February, 24, 9, 0, 0, 0, time.UTC))
	indexer.AddValue("frame3", "field1", 0, 100)

	err = indexer.Close()
	if err != nil {
		t.Fatalf("closing indexer: %v", err)
	}

	client, err := gopilosa.NewClient(hosts)
	if err != nil {
		t.Fatalf("getting client: %v", err)
	}
	schema, err := client.Schema()
	if err != nil {
		t.Fatalf("getting schema: %v", err)
	}

	idxs := schema.Indexes()
	if len(idxs) != 1 {
		t.Fatalf("too many indexes: %v", idxs)
	}
	if idx, ok := idxs["newindex"]; !ok {
		t.Fatalf("index with wrong name: %v", idx)
	}

	if len(idxs["newindex"].Frames()) != 4 {
		t.Fatalf("wrong number of frames: %v", idxs["newindex"].Frames())
	}

	idx, err := schema.Index("newindex")
	if err != nil {
		t.Fatalf("getting index: %v", err)
	}
	frametime, err := idx.Frame("frametime")
	if err != nil {
		t.Fatalf("getting frame: %v", err)
	}
	resp, err := client.Query(frametime.Range(0, time.Date(2018, time.February, 21, 9, 0, 0, 0, time.UTC), time.Date(2018, time.February, 23, 9, 0, 0, 0, time.UTC)))
	if err != nil {
		t.Fatalf("executing range query: %v", err)
	}
	bits := resp.Result().Bitmap().Bits
	if len(bits) != 1 || bits[0] != 0 {
		t.Fatalf("unexpected bits from range query: %v", bits)
	}

	resp, err = client.Query(frametime.Range(0, time.Date(2018, time.February, 20, 9, 0, 0, 0, time.UTC), time.Date(2018, time.February, 21, 9, 0, 0, 0, time.UTC)))
	if err != nil {
		t.Fatalf("executing range query: %v", err)
	}
	bits = resp.Result().Bitmap().Bits
	if len(bits) != 0 {
		t.Fatalf("unexpected bits from empty range query: %v", bits)
	}

	resp, err = client.Query(frametime.Range(0, time.Date(2018, time.February, 20, 9, 0, 0, 0, time.UTC), time.Date(2018, time.February, 25, 9, 0, 0, 0, time.UTC)))
	if err != nil {
		t.Fatalf("executing range query: %v", err)
	}
	bits = resp.Result().Bitmap().Bits
	if len(bits) != 2 || bits[1] != 2 || bits[0] != 0 {
		t.Fatalf("unexpected bits from empty range query: %v", bits)
	}

	frame3, err := idx.Frame("frame3")
	if err != nil {
		t.Fatalf("getting frame: %v", err)
	}
	field := frame3.Field("field1")

	resp, err = client.Query(field.Equals(100))
	if err != nil {
		t.Fatalf("executing range query: %v", err)
	}
	bits = resp.Result().Bitmap().Bits
	if len(bits) != 1 || bits[0] != 0 {
		t.Fatalf("unexpected bits from range field query: %v", bits)
	}

}
