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

package pdk

type recordError struct {
	record interface{}
	err    error
}

// PeekingSource is a wrapper for Source which implements the
// Peeker interface by reading the next record from Source
// and caching it for the next call to Record().
type PeekingSource struct {
	recordChan chan chan recordError
	peekChan   chan chan recordError
}

func (p *PeekingSource) Peek() (interface{}, error) {
	ch := make(chan recordError)
	p.peekChan <- ch
	re := <-ch
	return re.record, re.err
}

func (p *PeekingSource) Record() (interface{}, error) {
	ch := make(chan recordError)
	p.recordChan <- ch
	re := <-ch
	return re.record, re.err
}

func NewPeekingSource(source Source) *PeekingSource {
	ps := &PeekingSource{
		recordChan: make(chan chan recordError),
		peekChan:   make(chan chan recordError),
	}

	var cachedRecordError *recordError

	go func() {
		for {
			select {
			case rc := <-ps.recordChan:
				if cachedRecordError != nil {
					rc <- *cachedRecordError
					cachedRecordError = nil
				} else {
					r, e := source.Record()
					rc <- recordError{r, e}
				}
			case rc := <-ps.peekChan:
				if cachedRecordError != nil {
					rc <- *cachedRecordError
				} else {
					r, e := source.Record()
					cachedRecordError = &recordError{r, e}
					rc <- *cachedRecordError
				}
			}
		}
	}()
	return ps
}
