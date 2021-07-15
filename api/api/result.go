package api

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"

	//	"strconv"
	"time"

	"github.com/equinor/oneseismic/api/internal/auth"
	"github.com/equinor/oneseismic/api/internal/message"
	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
)

type Result struct {
	Timeout    time.Duration
	StorageURL string
	Storage    redis.Cmdable
	Keyring    *auth.Keyring
}

/*
 * Silly helper to centralise the name/key of the header object. It's not
 * likely to change too much, but it beats hardcoding the key with formatting
 * all over the place.
 */
func headerkey(pid string) string {
	return fmt.Sprintf("%s/header.json", pid)
}

func parseProcessHeader(doc []byte) (*message.ProcessHeader, error) {
	ph, err := (&message.ProcessHeader{}).Unpack(doc)
	if err != nil {
		log.Printf("bad process header: %s", string(doc))
		return ph, fmt.Errorf("unable to parse process header: %w", err)
	}

	if ph.Ntasks <= 0 {
		log.Printf("bad process header: %s", string(doc))
		return ph, fmt.Errorf("processheader.parts = %d; want >= 1", ph.Ntasks)
	}
	return ph, nil
}

func resultFromProcessHeader(
	head *message.ProcessHeader,
) *message.ResultHeader {
	return &message.ResultHeader{
		Bundles: head.Ntasks,
		Shape:   head.Shape,
		Index:   head.Index,
	}
}

func collectResult(
	ctx context.Context,
	storage redis.Cmdable,
	pid string,
	head *message.ProcessHeader,
	tiles chan []byte,
	failure chan error,
) {
	// This close is quite important - when the tiles channel is closed, it is
	// a signal to the caller that all partial results are in and processed,
	// and that the transfer is completed.
	defer close(tiles)

	rh := resultFromProcessHeader(head)
	rhpacked, err := rh.Pack()
	if err != nil {
		failure <- err
		return
	}
	tiles <- rhpacked

	streamCursor := "0"
	count := 0
	log.Printf("%s processing %d tasks...", pid, head.Ntasks)
	for count < head.Ntasks {
		xreadArgs := redis.XReadArgs{
			Streams: []string{pid, streamCursor},
			Block:   0,
		}
		reply, err := storage.XRead(ctx, &xreadArgs).Result()

		if err != nil {
			failure <- err
			return
		}

		for _, message := range reply[0].Messages {
			for key, tile := range message.Values {
				// If the stream includes a key named "error" something failed
				// when fetching fragments. Pass the error-text to failure-channel
				if key == "error" {
					failure <- errors.New(tile.(string))
					return
				}
				chunk, ok := tile.(string)
				if !ok {
					msg := fmt.Sprintf("tile.type = %T; expected []byte]", tile)
					failure <- errors.New(msg)
					return
				}

				tiles <- []byte(chunk)
				count++
				log.Printf("%s done %d", pid, count)
			}
			streamCursor = message.ID
		}
	}
	log.Printf("%s  collectResult done", pid)
}

func (r *Result) Stream(ctx *gin.Context) {
	pid := ctx.Param("pid")
	body, err := r.Storage.Get(ctx, headerkey(pid)).Bytes()
	if err != nil {
		log.Printf("Unable to get process header: %v", err)
		ctx.AbortWithStatus(http.StatusNotFound)
		return
	}

	head, err := parseProcessHeader(body)
	if err != nil {
		log.Printf("pid=%s, %v", pid, err)
		ctx.AbortWithStatus(http.StatusInternalServerError)
		return
	}

	tiles := make(chan []byte)
	failure := make(chan error)
	go collectResult(ctx, r.Storage, pid, head, tiles, failure)

	w := ctx.Writer
	header := w.Header()
	header.Set("Transfer-Encoding", "chunked")
	header.Set("Content-Type", "text/html") // TODO: Uhh.... text/html ??
	// See https://stackoverflow.com/a/62503611
	// TODO: Can we use this for final status?
	// The practical problem is that AFAICS no Python client-lib handles trailer-headers
	//header.Set("Trailer", "X-OnePac-Status")
	//header.Set("X-OnePac-Status", "streaming")
	w.WriteHeader(http.StatusOK)

	for {
		select {
		case output, ok := <-tiles:
			if !ok {
				log.Printf("pid=%s finished - flushing and closing", pid)
				//header.Set("X-OnePac-Status", "done")
				w.(http.Flusher).Flush()
				return
			}
			// To allow the client to handle chunking in the http-layer
			// we include the length of the payload. This is because
			// msgpack requires the complete bytearray when unpacking.
			// To make this simple, length is represented as a 10-character
			// string, sufficient to represent any 32bit integer.
			w.Write(append([]byte(fmt.Sprintf("%010d", (10+len(output)))), output...))
			w.(http.Flusher).Flush() // Perhaps not necessary?

		case err := <-failure:
			log.Printf("pid=%s, failure in STREAM: %s", pid, err)
			ctx.AbortWithStatus(http.StatusInternalServerError)
			w.(http.Flusher).Flush()
			//header.Set("X-OnePac-Status", err.Error())
			return
		}
	}
	// Should actually never get here...
	//header.Set("X-OnePac-Status", "done")
}

func (r *Result) Get(ctx *gin.Context) {
	pid := ctx.Param("pid")
	body, err := r.Storage.Get(ctx, headerkey(pid)).Bytes()
	if err != nil {
		log.Printf("Unable to get process header: %v", err)
		ctx.AbortWithStatus(http.StatusNotFound)
		return
	}

	head, err := parseProcessHeader(body)
	if err != nil {
		log.Printf("pid=%s, %v", pid, err)
		ctx.AbortWithStatus(http.StatusInternalServerError)
		return
	}

	count, err := r.Storage.XLen(ctx, pid).Result()

	if count < int64(head.Ntasks) {
		ctx.AbortWithStatus(http.StatusAccepted)
		return
	}

	tiles := make(chan []byte, 1000)
	failure := make(chan error)
	go collectResult(ctx, r.Storage, pid, head, tiles, failure)

	result := make([]byte, 0)
	header := ctx.Writer.Header()

	count = 0
TILE_LOOP:
	for {
		select {
		case tile, ok := <-tiles:
			if !ok {
				log.Printf("pid=%s finished - assembling data and returning", pid)
				break TILE_LOOP
			}
			result = append(result, tile...)
			count++
		case err := <-failure:
			log.Printf("pid=%s, %s", pid, err)
			//header.Set("X-OnePac-Status", "error")
			ctx.AbortWithStatus(http.StatusInternalServerError)
			return
		}
	}

	// Double-check that we actually got the expected number of blocks
	// In fact, count should be equal to head.Ntasks+1 because we receive
	// the header separately first
	// TODO: Not sure if this check is necessary...  probably doesn't hurt
	// but I don't easily see how it could occur...
	if count < int64(head.Ntasks) {
		header.Set("X-OnePac-Status", "error")
		ctx.AbortWithStatus(http.StatusInternalServerError)
		return
	}

	//header.Set("X-OnePac-Status", "done")
	log.Printf("pid=%s, returning %v bytes data", pid, len(result))
	ctx.Data(http.StatusOK, "application/octet-stream", result)
}

func (r *Result) Status(ctx *gin.Context) {
	pid := ctx.Param("pid")
	/*
	 * There's an interesting timing issue here - if /result is called before
	 * the job is scheduled and the header written, it is considered pending.
	 *
	 * The fact that the token checks out means that it is essentially pending
	 * - it's enqueued, but no processing has started [1]. Also, partial
	 * results have a fairly short expiration set, and requests to /result
	 * after expiration would still carry a valid auth token.
	 *
	 * The fix here is probably to include created-at and expiration in the
	 * token as well - if the token checks out, but the header does not exist,
	 * the status is pending.
	 *
	 * [1] the header-write step not completed, to be precise
	 */
	body, err := r.Storage.Get(ctx, headerkey(pid)).Bytes()
	if err == redis.Nil {
		/* request sucessful, but key does not exist */
		ctx.JSON(http.StatusAccepted, gin.H{
			"location": fmt.Sprintf("result/%s/status", pid),
			"status":   "pending",
		})
		return
	}
	if err != nil {
		log.Printf("%s %v", pid, err)
		ctx.AbortWithStatus(http.StatusInternalServerError)
		return
	}

	proc, err := parseProcessHeader(body)
	if err != nil {
		log.Printf("%s %v", pid, err)
		ctx.AbortWithStatus(http.StatusInternalServerError)
		return
	}

	count, err := r.Storage.XLen(ctx, pid).Result()
	if err != nil {
		log.Printf("%s %v", pid, err)
		ctx.AbortWithStatus(http.StatusInternalServerError)
		return
	}

	done := count == int64(proc.Ntasks)
	completed := fmt.Sprintf("%d/%d", count, proc.Ntasks)

	// TODO: add (and detect) failed status
	if done {
		ctx.JSON(http.StatusOK, gin.H{
			"location": fmt.Sprintf("result/%s", pid),
			"status":   "finished",
			"progress": completed,
		})
	} else {
		ctx.JSON(http.StatusAccepted, gin.H{
			"location": fmt.Sprintf("result/%s/status", pid),
			"status":   "working",
			"progress": completed,
		})
	}
}
