package main

import (
	"cloud.google.com/go/spanner"
	apiv1 "cloud.google.com/go/spanner/apiv1"
	"context"
	"errors"
	"fmt"
	"github.com/googleapis/gax-go/v2"
	"github.com/googleapis/gax-go/v2/apierror"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	sppb "google.golang.org/genproto/googleapis/spanner/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"log"
	"strings"
	"time"
)

func main() {

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	project := ""
	instance := ""
	database := ""

	dbPath := fmt.Sprintf("projects/%v/instances/%v/databases/%v", project, instance, database)

	md := metadata.New(map[string]string{
		"google-cloud-resource-prefix": dbPath,
	})

	api, err := apiv1.NewClient(context.Background())
	if err != nil {
		log.Fatalf("failed to create apiv1 client: %v", err)
	}

	// fails isSessionNotFound with client info set
	api.SetGoogleClientInfo("gccl", "1.41.0")
	req := sppb.CreateSessionRequest{
		Database: dbPath,
		Session:  &sppb.Session{Labels: map[string]string{}},
	}
	var hmd metadata.MD
	s, err := api.CreateSession(contextWithOutgoingMetadata(ctx, md), &req, gax.WithGRPCOptions(grpc.Header(&hmd)))
	if err != nil {
		log.Fatalf("failed to create session: %v", err)
	}

	err = api.DeleteSession(contextWithOutgoingMetadata(ctx, md), &sppb.DeleteSessionRequest{
		Name: s.GetName(),
	})
	if err != nil {
		log.Fatalf("failed to delete session: %v", err)
	}

	_, err = api.GetSession(contextWithOutgoingMetadata(ctx, md), &sppb.GetSessionRequest{Name: s.GetName()})

	if apie, ok := err.(*apierror.APIError); ok {
		if apie.GRPCStatus().Code() == codes.NotFound &&
			strings.HasPrefix(apie.GRPCStatus().Message(), "Session not found:") &&
			!isSessionNotFoundError(err) {
			log.Fatalf("Session not found error failed isSessionNotFoundError: %v", err)
		}
	}

	log.Printf("Ok: %s", dbPath)

}

func contextWithOutgoingMetadata(ctx context.Context, md metadata.MD) context.Context {
	existing, ok := metadata.FromOutgoingContext(ctx)
	if ok {
		md = metadata.Join(existing, md)
	}
	return metadata.NewOutgoingContext(ctx, md)
}

const sessionResourceType = "type.googleapis.com/google.spanner.v1.Session"

// isSessionNotFoundError returns true if the given error is a
// `Session not found` error.
func isSessionNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	if spanner.ErrCode(err) == codes.NotFound {
		if rt, ok := extractResourceType(err); ok {
			return rt == sessionResourceType
		}
	}
	return false
}

// extractResourceType extracts the resource type from any ResourceInfo detail
// included in the error.
func extractResourceType(err error) (string, bool) {
	var s *status.Status
	var se *spanner.Error
	if errorAs(err, &se) {
		// Unwrap statusError.
		s = status.Convert(se.Unwrap())
	} else {
		s = status.Convert(err)
	}
	if s == nil {
		return "", false
	}
	for _, detail := range s.Details() {
		if resourceInfo, ok := detail.(*errdetails.ResourceInfo); ok {
			return resourceInfo.ResourceType, true
		}
	}
	return "", false
}

// errorAs is a generic implementation of
// (errors|xerrors).As(error, interface{}). This implementation uses errors and
// is included in Go 1.13 and later builds.
func errorAs(err error, target interface{}) bool {
	return errors.As(err, target)
}
